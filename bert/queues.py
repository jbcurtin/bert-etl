import boto3
import copy
import hashlib
import logging
import json
import time
import typing
import uuid

from bert import \
    encoders as bert_encoders, \
    datasource as bert_datasource, \
    constants as bert_constants

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
PWN = typing.TypeVar('PWN')
DELAY: int = 15

class QueueItem:
    __slots__ = ('_payload', '_identity')
    _payload: typing.Dict[str, typing.Any]
    _identity: str
    def __init__(self: PWN, payload: typing.Dict[str, typing.Any], identity: str = None) -> None:
        self._payload = payload
        self._identity = identity

    def calc_identity(self: PWN) -> str:
        if self._identity:
            return self._identity

        combined: str = ''.join(bert_encoders.encode_identity_object(self._payload))
        combined: str = f'{combined}-{uuid.uuid4()}'
        return hashlib.sha256(combined.encode(bert_constants.ENCODING)).hexdigest()

    def keys(self: PWN) -> typing.Any:
        return super(QueueItem, self).keys()

    def get(self: PWN, name: str, default: typing.Any = None) -> typing.Any:
        return self._payload.get(name, default)

    def clone(self: PWN) -> typing.Any:
        return self.__class__(copy.deepcopy(self._payload))

    def __getitem__(self: PWN, name: str) -> typing.Any:
        try:
            return self._payload[name]
        except KeyError:
            raise KeyError(f'key-name[{name}] not found')

    def __setitem__(self: PWN, name: str, value: typing.Any) -> None:
        self._payload[name] = value

    def __delitem__(self: PWN, name: str) -> None:
        try:
            del self._payload[name]
        except KeyError:
            raise KeyError(f'key-name[{name}] not found')

class BaseQueue:
    _table_name: str
    _value: QueueItem
    def __init__(self: PWN, table_name: str) -> None:
        self._table_name = table_name
        self._value = None

    def __next__(self) -> typing.Any:
        if not self._value is None:
            logger.debug('Destroying Value')
            self._destroy(self._value)
            self._value = None

        self._value = self.get()
        if self._value is None or self._value == 'STOP':
            raise StopIteration

        return self._value

    def get(self: PWN) -> QueueItem:
        raise NotImplementedError

    def put(self: PWN, value: typing.Union[typing.Dict[str, typing.Any], QueueItem]) -> None:
        raise NotImplementedError

    def __iter__(self) -> PWN:
        return self

    def _destroy(self: PWN, queue_item: QueueItem) -> None:
        raise NotImplementedError

    def size(self: PWN) -> str:
        raise NotImplementedError


class DynamodbQueue(BaseQueue):
    _dynamodb_client: 'boto3.client("dynamodb")'
    def __init__(self: PWN, table_name: str) -> None:
        super(DynamodbQueue, self).__init__(table_name)
        self._dynamodb_client = boto3.client('dynamodb')

    def _destroy(self: PWN, queue_item: QueueItem, confirm_delete: bool = False) -> None:
        if confirm_delete:
            self._dynamodb_client.delete_item(
                TableName=self._table_name,
                Key={'identity': {'S': queue_item.calc_identity()}},
                Expected={'identity': {'Exists': True, 'Value': value['identity']}})

        else:
            self._dynamodb_client.delete_item(
                TableName=self._table_name,
                Key={'identity': {'S': queue_item.calc_identity()}})

    def put(self: PWN, value: typing.Union[typing.Dict[str, typing.Any], QueueItem]) -> None:
        if isinstance(value, dict):
            queue_item = QueueItem(value)

        elif isinstance(value, QueueItem):
            queue_item = value

        else:
            raise NotImplementedError

        encoded_value = bert_encoders.encode_object({
            'identity': queue_item.calc_identity(),
            'datum': queue_item.clone(),
        })
        self._dynamodb_client.put_item(TableName=self._table_name, Item=encoded_value)

    def get(self: PWN) -> typing.Dict[str, typing.Any]:
        try:
            value: typing.Any = self._dynamodb_client.scan(TableName=self._table_name, Select='ALL_ATTRIBUTES', Limit=1)['Items'][0]
        except IndexError:
            return None

        else:
            queue_item = QueueItem(bert_encoders.decode_object(value['datum']), value['identity']['S'])
            if value['identity']['S'] in ['sns-entry', 'invoke-arg', 'api-gateway', 'cognito']:
                return queue_item

            # The order of data when coming out of the database maynot be preserved, resulting in a different identity
            # assert queue_item.calc_identity() == value['identity']['S'], f'{queue_item.calc_identity()} != {value["identity"]["S"]}'
            return queue_item

class RedisQueue(BaseQueue):
    _table_name: str
    _redis_client: 'redis-client'
    def __init__(self, table_name: str) -> None:
        super(RedisQueue, self).__init__(table_name)
        self._redis_client = bert_datasource.RedisConnection.ParseURL(bert_constants.REDIS_URL).client

    def flushdb(self) -> None:
        self._redis_client.flushdb()

    def _destroy(self: PWN, queue_item: QueueItem) -> None:
        pass

    def size(self: PWN) -> int:
        return int(self._redis_client.llen(self._table_name))

    def get(self) -> QueueItem:
        try:
            value: str = self._redis_client.lpop(self._table_name).decode(bert_constants.ENCODING)
        except AttributeError:
            return 'STOP'

        else:
            # if self._cache_backend.has(value):
            #     return self._cache_backend.obtain(value)

            return bert_encoders.decode_object(json.loads(value)['datum'])

    def put(self: PWN, value: typing.Dict[str, typing.Any]) -> None:
        encoded_value = json.dumps(bert_encoders.encode_object({
            'identity': 'local-queue',
            'datum': value
        })).encode(bert_constants.ENCODING)
        # self._cache_backend.store(encoded_value)
        self._redis_client.rpush(self._table_name, encoded_value)


class StreamingQueue(DynamodbQueue):
    """
    When deploying functions to AWS Lambda, auto-invocation is available as an option to run the functions. With StreamingQueue, we want to push local objects into
        the available API already utilized. We also want to keep the available `put` function so that the `done_queue` api will still push contents into the next `work_queue`.
        We'll also argment the local `get` function api and only pull from records local to the stream and not pull from dynamodb.
    """
    # Share the memory across invocations, within the same process/thread. This allows for
    #   comm_binders to be called multipule-times and still pull from the same queue
    _queue: typing.List[typing.Dict[str, typing.Any]] = []
    def local_put(self: PWN, record: typing.Union[typing.Dict[str, typing.Any], QueueItem]) -> None:
        if isinstance(record, dict):
            queue_item = QueueItem(bert_encoders.decode_object(record['datum']), record['identity']['S'])

        elif isinstance(record, QueueItem):
            queue_item = record

        self._queue.append(queue_item)

    def get(self: PWN) -> QueueItem:
        try:
            value: QueueItem = self._queue.pop(0)
        except IndexError:
            # return super(StreamingQueue, self).get()
            return None

        else:
            return value

class LocalQueue(DynamodbQueue):
    """
    When testing, its convenient to use only a LocalQueue
    """
    _key: str = None
    # Share the memory across invocations, within the same process/thread. This allows for
    #   comm_binders to be called multipule-times and still pull from the same queue
    _queue: typing.List[typing.Dict[str, typing.Any]] = []
    def __init__(self: PWN, key: str) -> None:
        self._key = key
        self._value = None

    def local_put(self: PWN, record: typing.Dict[str, typing.Any]) -> None:
        self._queue.append(copy.deepcopy(record))

    def put(self: PWN, record: typing.Dict[str, typing.Any]) -> None:
        logger.info(f'LocalQueue Put[{record}]')

    def get(self: PWN) -> typing.Dict[str, typing.Any]:
        try:
            # may need to unpack because local queues are used for debugging in AWS Lambda
            value: typing.Any = self._queue.pop(0)
        except IndexError:
            return None

        else:
            return value

