import marshmallow
import marshmallow.fields
import json
import requests
import requests.auth
import typing

from bert import remote_manager

class CallbackSchema(marshmallow.Schema):
  callback_url: str = marshmallow.fields.String(required=True)
  callback_token: str = marshmallow.fields.String(required=True)

def submit(details: typing.Dict[str, typing.Any], service_name: str) -> None:
  url, token = remote_manager.find_service(service_name)
  # url: str = details['callback_url']
  # token: str = details['callback_token']
  class TokenAuth(requests.auth.AuthBase):
    def __call__(self, request) -> None:
      awe = details
      request.headers['Authorization'] = f'Bearer {token}'
      return request

  del details['callback_url']
  del details['callback_token']

  params: str = json.dumps(details)
  response = requests.post(url, data=params, headers={
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  }, auth=TokenAuth())
  import ipdb; ipdb.set_trace()
  pass

SERVICES: typing.Dict[str, typing.Any] = {}

def queue(service_name: str, details: typing.Dict[str, typing.Any]) -> None:
  url, token = remote_manager.find_service(service_name)
  import ipdb; ipdb.set_trace()
  pass

def register_service(service_name: str, service_url: str) -> None:
  if service_name in SERVICES.keys():
    raise NotImplementedError(f'Duplicate Service Name[{service_name}]')

  SERVICES[service_name] = {
    'base_url': service_url
  }
  import ipdb; ipdb.set_trace()
  pass
