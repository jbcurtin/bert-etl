import boto3
import logging
import typing
import uuid

logger = logging.getLogger(__name__)
PWN: typing.TypeVar = typing.TypeVar('PWN')

def aws_account_id() -> str:
    sts_client = boto3.client('sts')
    return sts.get_caller_identity()['Account']

def map_iam_role_by_arn(role_arn: str) -> typing.Dict[str, typing.Any]:
    if role_arn is None:
        return {'Arn': None}

    iam_client = boto3.client('iam')
    for page in iam_client.get_paginator('list_roles').paginate(PathPrefix='/'):
        for role in page['Roles']:
            if role_arn == role['Arn']:
                return role

    # Match the interface expected
    return {'Arn': None}

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

