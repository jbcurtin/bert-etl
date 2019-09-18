import marshmallow
import marshmallow.fields
import json
import requests
import requests.auth
import typing

from bert import remote_manager, remote_utils

PWN: typing.TypeVar = typing.TypeVar('PWN')
class CallbackSchema(marshmallow.Schema):
  callback_url: str = marshmallow.fields.String(required=True)
  callback_token: str = marshmallow.fields.String(required=True)

def submit(service_name: str, details: typing.Dict[str, typing.Any]) -> None:
  config: remote_utils.RemoteConfig = remote_utils.RemoteConfig.Obtain()
  class TokenAuth(requests.auth.AuthBase):
    def __call__(self, request) -> None:
      request.headers['Authorization'] = f'Bearer {config.auth_token}'
      return request

  params: str = json.dumps(details)
  response = requests.post(config.callback_url, data=params, headers={
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  }, auth=TokenAuth())
  response.raise_for_status()

def queue(service_name: str, details: typing.Dict[str, typing.Any]) -> None:
  url: str = remote_manager.SERVICE_CONNECTIONS[service_name]['queue_url']
  token: str = remote_manager.SERVICE_CONNECTIONS[service_name]['auth_token']
  class TokenAuth(requests.auth.AuthBase):
    def __call__(self: PWN, request: 'Request') -> 'Request':
      request.headers['Authorization'] = f'Bearer {token}'
      return request

  params: str = json.dumps(details)
  response = requests.post(url, data=params, headers={
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  }, auth=TokenAuth())
  response.raise_for_status()

