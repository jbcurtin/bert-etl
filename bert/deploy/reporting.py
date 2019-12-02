import boto3
import os
import json
import logging
import time
import types
import typing
import uuid

from bert import constants as bert_constants

from botocore.errorfactory import ClientError

from datetime import datetime, timedelta

TABLE_NAME: str = 'bert-etl-reporting'
PWN: typing.TypeVar = typing.TypeVar('PWN')
MONITOR_NAME: str = 'bert-etl-monitor'
logger = logging.getLogger(__name__)


def monitor_function_progress(module_name: str = None) -> None:
    import boto3
    from bert import \
            utils as bert_utils, \
            constants as bert_constants

    if module_name is None:
        logger.info(f'Using Bert Module Name[{MONITOR_NAME}]')
        module_name = os.environ.get('BERT_MODULE_NAME', None)
        if module_name is None:
            raise NotImplementedError

    dynamodb_client = boto3.client('dynamodb')
    lambda_client = boto3.client('lambda')
    time_offset: int = 15
    logger.info('Running Monitor function')
    jobs: typing.Dict[str, typing.Any] = bert_utils.scan_jobs(module_name)
    jobs: typing.Dict[str, typing.Any] = bert_utils.map_jobs(jobs, module_name)
    logger.info(f"Jobs Found[{','.join(jobs.keys())}]")
    for job_name, conf in jobs.items():
        logger.info(f'Processing Job[{job_name}]')
        scan_filter = {
            'job_name': {
                'AttributeValueList': [{'S': job_name}],
                'ComparisonOperator': 'EQ'
            }
        }
        # Clean out the reporting table of all stale entries. Throw an error in dynamodb_client with expected, if this doesn't work as expected.
        procd_jobs: typing.List[typing.Dict[str, typing.Any]] = []
        for page in dynamodb_client.get_paginator('scan').paginate(ConsistentRead=True, TableName=TABLE_NAME, ScanFilter=scan_filter):
            for item in page['Items']:
                created: datetime = datetime.strptime(item['created']['S'], bert_constants.REPORTING_TIME_FORMAT)
                if created < datetime.utcnow() - timedelta(minutes=time_offset):
                    dynamodb_client.delete_item(
                            TableName=TABLE_NAME,
                            Key={'identity': {'S': item['identity']['S']}},
                            Expected={'identity': {'Exists': True, 'Value': {'S': item['identity']['S']}}})
                else:
                    procd_jobs.append({'created': created, 'item': item})

        logger.info(f"Proc'd jobs found[{len(procd_jobs)}] for Job[{job_name}]")
        monitor_items = dynamodb_client.scan(ConsistentRead=True, TableName=TABLE_NAME, ScanFilter=scan_filter, Limit=1)['Items']
        logger.info(f"Monitor items found[{len(monitor_items)}] for Job[{job_name}]")
        if len(monitor_items) == 0:
            try:
                work_items = dynamodb_client.scan(ConsistentRead=True, TableName=conf['spaces']['work-key'], Limit=1)['Items']
            except dynamodb_client.exceptions.ResourceNotFoundException:
                work_items = []

            logger.info(f"Work items found[{len(work_items)}] for Job[{job_name}]")
            if len(work_items) > 0:
                try:
                    lambda_client.get_function(FunctionName=job_name)
                except lambda_client.exceptions.ResourceNotFoundException:
                    logger.info(f"Job[{job_name}] Lambda doesn't exist")
                else:
                    logger.info(f"Restarting Job[{job_name}]")

                    if conf['spaces']['pipeline-type'] == bert_constants.PipelineType.CONCURRENT:
                        for idx in range(0, (len(procd_jobs) - conf['spaces']['min_proced_items']) * -1):
                            lambda_client.invoke(FunctionName=job_name, InvocationType='Event', Payload=b'{}')

                    elif conf['spaces']['pipeline-type'] == bert_constants.PipelineType.BOTTLE:
                        if len(procd_jobs) == 0:
                            lambda_client.invoke(FunctionName=job_name, InvocationType='Event', Payload=b'{}')

                    else:
                        raise NotImplementedError


class track_execution():
    def __init__(self: PWN, job: types.FunctionType, **kwargs) -> None:
        self._client = boto3.client('dynamodb')
        self._job = job
        self._identity = str(uuid.uuid4())

    def __enter__(self: PWN) -> PWN:
        value = {
            'identity': {'S': self._identity},
            'job_name': {'S': self._job.__name__},
            'created': {'S': datetime.utcnow().strftime(bert_constants.REPORTING_TIME_FORMAT)}
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
                logger.info(f'Parent[] Is Running: False')
                return False

            time.sleep(.1)

        logger.info(f'Parent[] Is Running: True')
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
                logger.info(f'Self[{self._job.__name__}] IsRunning: False')
                return False

            time.sleep(.1)

        logger.info(f'Self[{self._job.__name__}] IsRunning: True')
        return True

