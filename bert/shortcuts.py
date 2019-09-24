import boto3
import collections
import logging
import os
import typing
import yaml

from bert import \
    exceptions as bert_exceptions

from botocore.errorfactory import ClientError

logger = logging.getLogger(__name__)

def head_bucket_for_existance(bucket_name: str) -> None:
    if bucket_name is None:
        return None

    client = boto3.client('s3')
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError as err:
        if '(403)' in err.args[0]:
            raise bert_exceptions.AWSError(f'Bucket[{bucket_name}] name is taken by someone else')

def obtain_deployment_config(bert_configuration: typing.Any) -> collections.namedtuple:
    """
    Written verbosely to allow for default_keys
    """
    defaults = {
        's3_bucket': None
    }
    items = bert_configuration.get('deployment', {})
    for key, value in defaults.items():
        if not key in items.keys():
            items[key] = value

    keys = [key for key in items.keys()]
    values = []
    deployment_tuple = collections.namedtuple('Deployment', keys)
    for key in keys:
        values.append(items[key])

    return deployment_tuple(*values)

def load_configuration() -> typing.Dict[str, typing.Any]:
    conf_path: str = os.path.join(os.getcwd(), 'bert-etl.yaml')
    if not os.path.exists(conf_path):
        logger.info(f"Configuration Path[{conf_path}] doesn't exist")
        return None

    with open(conf_path, 'r', encoding='utf-8') as stream:
        return yaml.load(stream.read(), Loader=yaml.FullLoader)

def merge_lists(main: typing.List[typing.Any], secondary: typing.List[typing.Any], defaults: typing.List[typing.Any]) -> typing.List[typing.Any]:
    if len(main) == 0 and len(secondary) == 0:
        return defaults

    # Preserve order or list
    merged = []
    for value in secondary:
        if value in merged:
            continue

        merged.append(value)

    for value in main:
        if value in merged:
            continue

        merged.append(value)

    return merged

def merge_env_vars(defaults: typing.Dict[str, str], env_vars: typing.Dict[str, str]) -> typing.Dict[str, str]:
    merged: typing.Dict[str, str] = {key: value for key, value in defaults.items()}
    for key, value in env_vars.items():
        merged[key] = value

    return merged

def merge_requirements(defaults: typing.List[str], requirements: typing.List[str]) -> typing.List[str]:
    merged: typing.List[str] = {value for value in defaults}
    for value in requirements:
        if value in merged:
            continue

        merged.append(value)

    return [item for item in merged]

def get_if_exists(key: str, default: typing.Any, data_type: typing.Any, defaults: typing.Dict[str, str], env_vars: typing.Dict[str, str]) -> typing.Dict[str, str]:
    try:
        return data_type(env_vars[key])
    except KeyError:
        pass

    except ValueError:
        raise bert_exceptions.BertConfigError(f'Key[{key}] is not DataType[{data_type}]')

    try:
        return data_type(defaults.get(key, default))
    except ValueError:
        raise bert_exceptions.BertConfigError(f'Key[{key}] is not DataType[{data_type}]')

