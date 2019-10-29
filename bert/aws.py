import boto3
import os
import typing
import uuid

from botocore.exceptions import ClientError

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

