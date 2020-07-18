import boto3
import enum
import logging
import json
import os
import tempfile
import typing

from bert.etl.security import AccessLevel

ENCODING = 'utf-8'
logger = logging.getLogger(__name__)
def upload_dataset(dataset: typing.Any, s3_key: str, bucket_name: str, access_level: AccessLevel = None) -> None:
    filepath = tempfile.NamedTemporaryFile().name
    with open(filepath, 'wb') as stream:
        stream.write(json.dumps(dataset).encode(ENCODING))

    logger.info(f'Uploading Dataset[{s3_key}]')
    s3_client = boto3.client('s3')
    if access_level:
        s3_client.upload_file(filepath, bucket_name, s3_key, ExtraArgs={'ACL': access_level.value})

    else:
        s3_client.upload_file(filepath, bucket_name, s3_key)

def download_dataset(s3_key: str, bucket_name: str, expected_datastructure: typing.Any = list) -> typing.Any:
    filepath = tempfile.NamedTemporaryFile().name
    logger.info(f'Downloading Dataset[{s3_key}]')
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket_name, s3_key, filepath)
    if expected_datastructure in [list, dict]:
        with open(filepath, 'rb') as stream:
            result = json.loads(stream.read().decode(ENCODING))
            if isinstance(result, expected_datastructure):
                return result
            else:
                raise NotImplementedError(f'Expected Datatype[{expected_datastructure}] miss-match')

    else:
        raise NotImplementedError(f'Unexpeceted DataStructure[{expected_datastructure}]')

