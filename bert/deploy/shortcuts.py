import boto3
import logging
import os
import typing
import yaml

logger = logging.getLogger(__name__)

def merge_env_vars(defaults: typing.Dict[str, str], env_vars: typing.Dict[str, str]) -> typing.Dict[str, str]:
    merged: typing.Dict[str, str] = {key: value for key, value in defaults.items()}
    for key, value in env_vars.items():
        merged[key] = value

    return merged

def load_local_configuration() -> typing.Dict[str, typing.Any]:
    conf_path: str = os.path.join(os.getcwd(), 'bert-etl.yaml')
    if not os.path.exists(conf_path):
        logger.info("Configuration Path[{conf_path}] doesn't exist")
        return None

    with open(conf_path, 'r', encoding='utf-8') as stream:
        return yaml.load(stream.read(), Loader=yaml.FullLoader)

def aws_account_id() -> str:
    sts_client = boto3.client('sts')
    return sts.get_caller_identity()['Account']

def map_iam_role(role_name: str) -> typing.Dict[str, typing.Any]:
    iam_client = boto3.client('iam')
    for page in iam_client.get_paginator('list_roles').paginate(PathPrefix='/'):
        for role in page['Roles']:
            if role['RoleName'] == role_name:
                return role

def map_iam_policy(policy_name: str) -> typing.Dict[str, typing.Any]:
    iam_client = boto3.client('iam')
    for page in iam_client.get_paginator('list_policies').paginate(PathPrefix='/'):
        for policy in page['Policies']:
            if policy['PolicyName'] == policy_name:
                return policy

