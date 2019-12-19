import boto3
import collections
import importlib
import json
import logging
import os
import types
import typing
import yaml

from bert import \
    exceptions as bert_exceptions

from botocore.errorfactory import ClientError

from json.decoder import JSONDecodeError

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

def obtain_packages(bert_configuration: typing.Any) -> collections.namedtuple:
    binary_def = collections.namedtuple('Binary', ['path', 'name'])
    binary_paths: typing.List[binary_def] = []

    for job_name, conf in bert_configuration.items():
        if job_name.lower() == 'deployment':
            continue

        for entry in conf.get('binary_paths', []):
            binary_paths.append(binary_def(entry['local_path'], entry['binary_name']))

    return binary_paths

def obtain_secrets_config(bert_configuration: typing.Any) -> collections.namedtuple:
    defaults = {
        'key_alias': 'bert-etl',
        'usernames': [],
    }
    items = bert_configuration.get('secrets', {})
    for key, value in defaults.items():
        if not key in items.keys():
            items[key] = value

    keys = [key for key in items.keys()]
    values = []
    secrets_tuple = collections.namedtuple('Secrets', keys)
    for key in keys:
        values.append(items[key])

    return secrets_tuple(*values)

def obtain_deployment_config(bert_configuration: typing.Any) -> collections.namedtuple:
    """
    Written verbosely to allow for default_keys
    """
    defaults = {
        's3_bucket': None,
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

def save_configuration(configuration: typing.Dict[str, typing.Any]) -> None:
    conf_path: str = os.path.join(os.getcwd(), 'bert-etl.yaml')
    logger.info(f'Writing configuration to path[{conf_path}]')
    with open(conf_path, 'w', encoding='utf-8') as stream:
        stream.write(yaml.dump(configuration, indent=4, canonical=False))

def load_configuration() -> typing.Dict[str, typing.Any]:
    conf_path: str = os.path.join(os.getcwd(), 'bert-etl.yaml')
    if not os.path.exists(conf_path):
        logger.info(f"Configuration Path[{conf_path}] doesn't exist")
        return None

    with open(conf_path, 'r', encoding='utf-8') as stream:
        return yaml.load(stream.read(), Loader=yaml.FullLoader)

def get_and_merge_if_exists(keypath: str, default: typing.Any, object_type: typing.Any, defaults: typing.List[typing.Any], primary: typing.List[str]) -> typing.List[str]:
    steps: typing.List[str] = keypath.split('.')
    if len(steps) > 1:
        for key in steps[:-1]:
            try:
                if isinstance(defaults[key], (dict, list)):
                    defaults = defaults[key]

                else:
                    raise bert_exceptions.BertConfigError(f'Invalid KeyPath for Defaults: keypath[{keypath}], key[{key}]')

            except KeyError:
                pass

            try:
                if isinstance(primary[key], (dict, list)):
                    primary = primary[key]

                else:
                    raise bert_exceptions.BertConfigError(f'Invalid KeyPath for Primary: keypath[{keypath}], key[{key}]')

            except KeyError:
                pass
                # raise bert_exceptions.BertConfigError(f'Invalid KeyPath: keypath[{keypath}], key[{key}]')

    values: typing.List[str] = defaults.get(steps[-1], [])
    values.extend(primary.get(steps[-1], []))
    return values


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

def get_if_exists(keypath: str, default: typing.Any, data_type: typing.Any, defaults: typing.Dict[str, str], input_vars: typing.Dict[str, str]) -> typing.Dict[str, str]:
    steps: typing.List[str] = keypath.split('.')
    if len(steps) > 1:
        for key in steps[:-1]:
            try:
                if isinstance(input_vars[key], (dict, list)):
                    input_vars = input_vars[key]

                else:
                    raise bert_exceptions.BertConfigError(f'Invalid KeyPath: keypath[{keypath}], key[{key}]')

            except KeyError:
                pass
                # raise bert_exceptions.BertConfigError(f'Invalid KeyPath: keypath[{keypath}], key[{key}]')

    key: str = steps[-1]
    try:
        return data_type(input_vars[key])
    except KeyError:
        pass

    except ValueError:
        raise bert_exceptions.BertConfigError(f'Key[{keypath}] is not DataType[{data_type}]')

    try:
        result: typing.Any = defaults.get(key, default)
        if result is None:
            return None

        return data_type(result)
    except ValueError:
        raise bert_exceptions.BertConfigError(f'Default Key[{keypath}] is not DataType[{data_type}]')


def _load_invoke_args_module(member_route: str) -> typing.Dict[str, typing.Any]:
    try:
        module_path, member_name = member_route.rsplit('.', 1)
        module = importlib.import_module(module_path)
    except ValueError:
        raise ImportError

    else:
        member: typing.Union[typing.Dict[str, typing.Any], types.FunctionType, None] = getattr(module, member_name, None)
        if member is None:
            raise ImportError

        elif isinstance(member, dict):
            return member

        elif isinstance(member, types.FunctionType):
            return member()

        else:
            raise NotImplementedError(member)

def load_invoke_args(invoke_args: typing.List[str]) -> typing.List[typing.Dict[str, typing.Any]]:
    loaded: typing.List[typing.Dict[str, typing.Any]] = []
    for invoke_item in invoke_args:
        if isinstance(invoke_item, dict):
            # yaml
            loaded.append(invoke_item)

        elif invoke_item.endswith('.json'):
            with open(invoke_item, 'r') as stream:
                loaded.append(json.loads(stream.read()))

        elif invoke_item.endswith('.yaml') or invoke_item.endswith('.yml'):
            with open(invoke_item, 'r') as stream:
                loaded.append(yaml.load(stream.read(), Loader=yaml.FullLoader))

        else:
            try:
                loaded.extend(_load_invoke_args_module(invoke_item))
            except ImportError:
                pass
            else:
                continue

            try:
                datum = json.loads(invoke_item)
                if isinstance(datum, list):
                    loaded.extend(datum)

                elif isinstance(datum, dict):
                    loaded.append(datum)

                else:
                    raise bert_exceptions.BertConfigError(f'Invalid Json Type. Only List, Dict are supported.')
            except JSONDecodeError:
                pass

            else:
                continue

            raise bert_exceptions.BertConfigError(f'Unable to load invoke_arg[{invoke_item}]')

    return loaded

def load_if_exists(keypath: str, configuration: typing.Dict[str, typing.Any]) -> typing.Any:
    steps = keypath.split('.')
    if len(steps) > 1:
        for key in steps[:-1]:
            try:
                if isinstance(configuration[key], (list, dict)):
                    configuration= configuration[key]

                else:
                    raise bert_exceptions.BertConfigError(f'Invalid Keypath: keypath[{keypath}]')
            except KeyError:
                pass

    try:
        return configuration[steps[-1]]
    except (KeyError, IndexError) as err:
        raise bert_exceptions.BertConfigError(f'Invalid Keypath: keypath[{keypath}]')

def write_keypath_value(keypath: str, configuration: typing.Dict[str, typing.Any], value: typing.Any) -> None:
    steps = keypath.split('.')
    if len(steps) > 1:
        for key in steps[:-1]:
            try:
                if isinstance(configuration[key], (list, dict)):
                    configuration = configuration[key]

                else:
                    raise bert_exceptions.BertConfigError(f'Invalid Keypath: keypath[{keypath}]')

            except KeyError:
                pass

    configuration[steps[-1]] = value

