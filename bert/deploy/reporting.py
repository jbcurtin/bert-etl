import boto3
import json
import logging
import types
import typing
import uuid

from botocore.errorfactory import ClientError

from datetime import datetime, timedelta

TABLE_NAME: str = 'bert-etl-reporting'
PWN: typing.TypeVar = typing.TypeVar('PWN')
logger = logging.getLogger(__name__)

class track_execution():
    def __init__(self: PWN, job: types.FunctionType, **kwargs) -> None:
        self._client = boto3.client('dynamodb')
        self._job = job
        self._identity = str(uuid.uuid4())

    def __enter__(self: PWN) -> PWN:
        value = {
            'identity': {'S': self._identity},
            'job': {'S': self._job.__name__},
        }
        self._client.put_item(TableName=TABLE_NAME, Item=value)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        try:
            self._client.delete_item(TableName=TABLE_NAME, Key={'identity': {'S': self._identity}})
        except ClientError as err:
            logger.info('ClientError Exception')
            logger.exception(err)

        if exception_value:
            logger.info('Exit Exception')
            raise exception_value

        return None

class manager():
    def __init__(self: PWN, job: types.FunctionType) -> None:
        self._client = boto3.client('dynamodb')
        self._job = job
        self._identity = str(uuid.uuid4())
        self._delay = 3

    def is_safe(self: PWN) -> bool:
        if self._is_parent_running() is False and self._is_running_already() is False:
            logger.info('Is Safe True')
            return True

        logger.info('Is Safe False')
        return False

    def _is_parent_running(self: PWN) -> bool:
        scan_filter = {
            'job_name': {
                'AttributeValueList': [
                    {'S': parent_func.__name__}
                    for parent_func in self._job.parent_funcs],
                'ComparisonOperator': 'IN',
            }
        }
        local_timeout: datetime = datetime.utcnow() + timedelta(seconds=self._delay)
        while local_timeout > datetime.utcnow():
            items: typing.List[typing.Any] = self._client.scan(Limit=1, TableName=TABLE_NAME,
                ScanFilter=scan_filter, ConsistentRead=True)['Items']

            if len(items) == 0:
                logger.info('Parent Is Running False')
                return False

            time.sleep(.1)

        logger.info('Parent Is Running True')
        return True

    def _is_running_already(self: PWN) -> bool:
        scan_filter = {
            'job_name': {
                'AttributeValueList': [{'S': self._job.__name__}],
                'ComparisonOperator': 'EQ',
            }
        }
        local_timeout: datetime = datetime.utcnow() + timedelta(seconds=self._delay)
        while local_timeout > datetime.utcnow():
            items: typing.List[typing.Any] = self._client.scan(Limit=1, TableName=TABLE_NAME,
                ScanFilter=scan_filter, ConsistentRead=True)['Items']

            if len(items) == 0:
                logger.info('IsRunning False')
                return False

            time.sleep(.1)

        logger.info('IsRunning True')
        return True

