import base64
import boto3
import json
import logging
import os
import typing
import uuid

from bert import \
    exceptions as bert_exceptions, \
    constants as bert_constants

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
PWN: typing.TypeVar = typing.TypeVar('PWN')

class assume_role:
    __slots__ = ('_role_arn', '_duration', '_session_name', '_env_vars', '_old_values')
    _env_vars: typing.Dict[str, str]
    _old_values: typing.Dict[str, str]
    _role_arn: str
    _duration: int
    _session_name: str

    def __init__(self: PWN, role_arn: str, duration: int = 3600) -> None:
        self._role_arn = role_arn
        self._duration = duration
        self._session_name = str(uuid.uuid4())
        self._env_vars = {}
        self._old_values = {}

    def __enter__(self: PWN) -> PWN:
        sts_client = boto3.client('sts')
        try:
            response = sts_client.assume_role(RoleArn=self._role_arn, RoleSessionName=self._session_name, DurationSeconds=self._duration)
        except ClientError as err:
            # This could happen for two reasons.
            # 1. The user is not registered as a trust entity
            # 2. The duration of the assume role is greater than what has been configured in IAM
            raise err

        self._env_vars['AWS_ACCESS_KEY_ID'] = response['Credentials']['AccessKeyId']
        self._env_vars['AWS_SECRET_ACCESS_KEY'] = response['Credentials']['SecretAccessKey']
        self._env_vars['AWS_SESSION_TOKEN'] = response['Credentials']['SessionToken']
        for key, value in self._env_vars.items():
            self._old_values[key] = os.environ.get(key, None)
            os.environ[key] = value

        return self

    def __exit__(self: PWN, exception_type: 'ExceptionType', exception_value: Exception, traceback: typing.Any) -> None:
        for key, old_value in self._old_values.items():
            if old_value is None:
                del os.environ[key]

            else:
                os.environ[key] = old_value

        if exception_value:
            raise exception_value

class kms:
    _kms_client: 'boto3.client("kms")'
    _alias: str
    _key: typing.Dict[str, typing.Any]
    _auto_create: bool
    _usernames: typing.List[str]
    _header: str = 'bert-etl-encrypted'
    class CipherEncryptionError(bert_exceptions.BertException):
        pass
    class CipherDecryptionError(bert_exceptions.BertException):
        pass
    class KeyNotFound(bert_exceptions.BertException):
        pass

    def __init__(self: PWN, alias: str, usernames: typing.List[str] = [], auto_create: bool = False) -> None:
        self._kms_client = boto3.client('kms')
        self._alias = alias
        self._key = None
        self._auto_create = auto_create
        self._usernames = usernames

    def _gen_key_policy(self: PWN) -> str:
        caller_info: typing.Dict[str, typing.Any] = boto3.client('sts').get_caller_identity()
        caller_username: str = caller_info['Arn'].rsplit(':', 1)[-1]
        usernames: typing.List[str] = list({username for username in self._usernames})
        if not caller_username in usernames:
            logger.info(f'Adding AWS Caller username[{caller_username}] to KMSKey[{self._alias}]')
            usernames.append(caller_username)

        if not 'root' in usernames:
            logger.info(f'Adding AWS Caller username[root] to KMSKey[{self._alias}] Usernames[{",".join(usernames)}]')
            usernames.append('root')

        return json.dumps({
            'Version': '2012-10-17',
            'Id' : 'key-default-1',
            'Statement' : [
                {
                    'Sid' : 'Enable IAM User Permissions',
                    'Effect' : 'Allow',
                    'Principal' : {
                        'AWS' : f'arn:aws:iam::{caller_info["Account"]}:{username}'
                    },
                    'Action' : 'kms:*',
                    'Resource' : '*'
                } for username in usernames]
        })

    def _load_key_by_alias(self: PWN, key_alias: str) -> typing.Dict[str, typing.Any]:
        for page in self._kms_client.get_paginator('list_aliases').paginate():
            for kms_alias in page['Aliases']:
                if kms_alias['AliasName'] == key_alias:
                    return self._kms_client.describe_key(KeyId=kms_alias['TargetKeyId'])

    def __enter__(self: PWN) -> PWN:
        alias_name: str = f'alias/{self._alias}'
        if self._key is None:
            self._key = self._load_key_by_alias(alias_name)

        if self._auto_create and self._key is None:
            key = self._kms_client.create_key(
                Description=f'{self._alias} Key',
                Origin="AWS_KMS",
                KeyUsage='ENCRYPT_DECRYPT')
            key_policy: str = self._gen_key_policy()
            self._kms_client.create_alias(
                AliasName=f'alias/{self._alias}',
                TargetKeyId=key['KeyMetadata']['KeyId'])
            self._kms_client.put_key_policy(
                KeyId=key['KeyMetadata']['KeyId'],
                PolicyName='default',
                Policy=key_policy)

            self._key = self._load_key_by_alias(alias_name)

        if self._key is None:
            raise self.__class__.KeyNotFound(self._alias)

        return self

    def __exit__(self: PWN, exception_type: 'ExceptionType', exception_value: Exception, traceback: typing.Any) -> None:
        if exception_value:
            raise exception_value

    def update_usernames(self: PWN) -> None:
        key_policy: str = self._gen_key_policy()
        self._kms_client.put_key_policy(
            KeyId=self._key['KeyMetadata']['KeyId'],
            PolicyName='default',
            Policy=key_policy)

    def decrypt(self: PWN, value: str) -> str:
        if not value.startswith(self._header):
            raise self.__class__.CipherDecryptionError('Value already decrypted')

        header, alias, cipher = value.split(':')
        encrypted_value: bytes = base64.b64decode(cipher.encode(bert_constants.ENCODING))
        try:
            decrypted_value: typing.Dict[str, typing.Any] = self._kms_client.decrypt(CiphertextBlob=encrypted_value)
        except self._kms_client.exceptions.InvalidCiphertextException:
            raise NotImplementedError

        return decrypted_value['Plaintext'].decode(bert_constants.ENCODING)

    def encrypt(self: PWN, value: str) -> str:
        if value.startswith(self._header):
            raise self.__class__.CipherEncryptionError('Value already encrypted')

        encrypted_value: typing.Dict[str, typing.Any] = self._kms_client.encrypt(
            KeyId=self._key['KeyMetadata']['KeyId'],
            Plaintext=value.encode(bert_constants.ENCODING))
        ascii_value: str = base64.b64encode(encrypted_value['CiphertextBlob']).decode(bert_constants.ENCODING)
        return f'{self._header}:{self._alias}:{ascii_value}'

