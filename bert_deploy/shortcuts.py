import boto3
import typing

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

