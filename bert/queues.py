import boto3
import copy
import hashlib
import logging
import json
import typing

from bert import \
    encoders as bert_encoders, \
    datasource as bert_datasource, \
    constants as bert_constants, \
    naming as bert_naming

# from botocore.errorfactory import ResourceNotFoundException

logger = logging.getLogger(__name__)
PWN = typing.TypeVar('PWN')

class DynamodbQueue:
    @staticmethod
    def Pack(datum: typing.Dict[str, typing.Any]) -> str:
        """
        Dict -> AWS Dict -> json.dumps
        """
        datum = bert_encoders.encode_object(datum)
        return datum

    @staticmethod
    def UnPack(datum: str) -> typing.Dict[str, typing.Any]:
        """
        AWS Dynamodb Dict -> Dict
        """
        return bert_encoders.decode_object(datum)

    _key: str = None
    def __init__(self: PWN, key: str) -> None:
        self._key = bert_naming.calc_table_name(key)

    def __iter__(self) -> PWN:
        return self

    def __next__(self) -> typing.Any:
        value = self.get()
        if value is None or value == 'STOP':
            raise StopIteration

        return value

    def put(self: PWN, value: typing.Dict[str, typing.Any]) -> None:
        combined: str = ''.join(sorted(bert_encoders.encode_identity_object(value)))
        identity: str = hashlib.sha256(combined.encode('utf-8')).hexdigest()
        value: str = self.__class__.Pack({'identity': identity, 'datum': copy.deepcopy(value)})
        client: typing.Any = boto3.client('dynamodb')
        client.put_item(TableName=self._key, Item=value)

    def get(self: PWN) -> typing.Dict[str, typing.Any]:
        client: typing.Any = boto3.client('dynamodb')
        try:
            value: typing.Any = client.scan(TableName=self._key, Select='ALL_ATTRIBUTES', Limit=1)['Items'][0]
        except IndexError:
            return None

        else:
            unpacked: typing.Dict[str, typing.Any] = self.__class__.UnPack(copy.deepcopy(value)['datum'])
            client.delete_item(TableName=self._key, Key={'identity': value['identity']})
            return unpacked

class StreamingQueue(DynamodbQueue):
    """
    When deploying functions to AWS Lambda, auto-invocation is available as an option to run the functions. With StreamingQueue, we want to push local objects into
        the available API already utilized. We also want to keep the available `put` function so that the `done_queue` api will still push contents into the next `work_queue`.
        We'll also argment the local `get` function api and only pull from records local to the stream and not pull from dynamodb.
    """
    _key: str = None
    # Share the memory across invocations, within the same process/thread. This allows for
    #   comm_binders to be called multipule-times and still pull from the same queue
    _queue: typing.List[typing.Dict[str, typing.Any]] = []
    def __init__(self: PWN, key: str) -> None:
        self._key = bert_naming.calc_table_name(key)

    def local_put(self: PWN, record: typing.Dict[str, typing.Any]) -> None:
        self._queue.append(copy.deepcopy(record))

    def get(self: PWN) -> typing.Dict[str, typing.Any]:
        try:
            value: typing.Any = self._queue.pop(0)
        except IndexError:
            return None

        else:
            unpacked: typing.Dict[str, typing.Any] = self.__class__.UnPack(copy.deepcopy(value)['datum'])
            client = boto3.client('dynamodb')
            # try:
            client.delete_item(TableName=self._key, Key={'identity': value['identity']})
            # except ResourceNotFoundException:
            #     # If the object doesn't exist in Dynamodb, it means another function has
            #     #  taken the object.
            #     return None

            return unpacked

class LocalQueue(DynamodbQueue):
    """
    When testing, its convenient to use only a LocalQueue
    """
    _key: str = None
    # Share the memory across invocations, within the same process/thread. This allows for
    #   comm_binders to be called multipule-times and still pull from the same queue
    _queue: typing.List[typing.Dict[str, typing.Any]] = []
    def __init__(self: PWN, key: str) -> None:
        self._key = bert_naming.calc_table_name(key)

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

class RedisQueue:
    @staticmethod
    def Pack(datum: typing.Dict[str, typing.Any]) -> str:
        return bert_encoders.encode_object(datum)

    @staticmethod
    def UnPack(datum: str) -> typing.Dict[str, typing.Any]:
        return bert_encoders.decode_object(datum)

    def __init__(self, redis_key: str):
        self._key = bert_naming.calc_table_name(redis_key)
        self._redis_client = bert_datasource.RedisConnection.ParseURL(bert_constants.REDIS_URL).client

    def __iter__(self):
        return self

    def __next__(self):
        value = self.get()
        if value is None or value == 'STOP':
            raise StopIteration

        return value

    def flushdb(self) -> None:
        self._redis_client.flushdb()

    def get(self) -> typing.Dict[str, typing.Any]:
        try:
            value: str = self._redis_client.lpop(self._key).decode(bert_constants.ENCODING)
        except AttributeError:
            return 'STOP'

        else:
            value: typing.Dict[str, typing.Any] = json.loads(value)
            return self.__class__.UnPack({'M': value})

    def put(self, value: typing.Dict[str, typing.Any]) -> None:
        value: typing.Dict[str, typing.Any] = self.__class__.Pack(value)
        value: str = json.dumps(value)
        self._redis_client.rpush(self._key, value.encode(bert_constants.ENCODING))

