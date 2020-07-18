import enum
import typing
import uuid

from bert import utils as bert_utils

PWN = typing.TypeVar('PWN')

class CognitoTrigger(enum.Enum):
    POST_AUTHENTICATION = 'post-authen'
    PRE_TOKEN_GENERATION = 'pre-token-generation'

class CognitoEventDefaults(typing.NamedTuple):
    region: str = 'us-east-1'
    userPoolId: str = None
    clientId: str = None
    username: str = str(uuid.uuid4())
    email: str = 'cog18@jbcurtin.io'
    name: str = 'Joe'
    phone_number: str = '+12406945744'

class CognitoEvent:
    def __init__(self: PWN, defaults: CognitoEventDefaults) -> None:
        self._defaults = defaults

    def trigger_content(self: PWN, trigger: CognitoTrigger) -> typing.Dict[str, typing.Any]:
        if trigger is CognitoTrigger.POST_AUTHENTICATION:
            return {
                'version': '1',
                'triggerSource': 'PostAuthentication_Authentication',
                'region': self._defaults.region,
                'userPoolId': self._defaults.userPoolId,
                'userName': self._defaults.username,
                'callerContext': {
                    'awsSdkVersion': 'aws-sdk-unknown-unknown',
                    'clientId': self._defaults.clientId,
                },
                'request': {
                    'userAttributes': {
                        'sub': self._defaults.username,
                        'email_verified': 'true',
                        'cognito:user_status': 'CONFIRMED',
                        'cognito:email_alias': self._defaults.email,
                        'name': 'Joe',
                        'phone_number_verified': 'false',
                        'phone_number': self._defaults.phone_number,
                        'email': self._defaults.email,
                    },
                    'newDeviceUsed': False,
                },
                'response': {}
            }

        elif trigger is CognitoTrigger.PRE_TOKEN_GENERATION:
            return {
                'version': '1',
                'triggerSource': 'TokenGeneration_Authentication',
                'region': self._defaults.region,
                'userPoolId': self._defaults.userPoolId,
                'userName': self._defaults.username,
                'callerContext': {
                    'awsSdkVersion': 'aws-sdk-unknown-unknown',
                    'clientId': self._defaults.clientId,
                },
                'request': {
                    'userAttributes': {
                        'sub': self._defaults.username,
                        'email_verified': 'true',
                        'cognito:user_status': 'CONFIRMED',
                        'cognito:email_alias': self._defaults.email,
                        'name': 'Joe',
                        'phone_number_verified': 'false',
                        'phone_number': self._defaults.phone_number,
                        'email': self._defaults.email,
                    },
                    'groupConfiguration': {
                        'groupsToOverride': [],
                        'iamRolesToOverride': [],
                        'preferredRole': None
                    },
                },
                'response': {
                    'claimsOverrideDetails': None
                },
            }

        else:
            raise NotImplementedError(trigger)

