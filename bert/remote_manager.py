import collections
import flask
import hashlib
import marshmallow
import marshmallow.fields
import json
import requests
import requests.auth
import typing

from bert import remote_exceptions, constants, remote_utils

SERVICE_CONNECTIONS: typing.Dict[str, typing.Any] = {}

class RegisterSchema(marshmallow.Schema):
  nonce: str = marshmallow.fields.String(required=True)

def register(
  app: flask.Flask,
  service_name: str,
  service_entropy: str,
  service_queue_url: str,
  service_shake_url: str,
  callback_url: str) -> 'Route Auth':

  if service_name in SERVICE_CONNECTIONS.keys():
    raise NotImplementedError(f'Duplicate ServiceName[{service_name}]')

  SERVICE_CONNECTIONS[service_name] = {
    'auth_token': remote_utils.build_auth_token(service_name, service_entropy),
    'queue_url': service_queue_url,
    'shake_url': service_shake_url,
    'callback_url': callback_url
  }
  @app.route(f'/{service_name}.register', methods=['POST'])
  def register_service() -> flask.Response:
    if flask.request.json:
      result = RegisterSchema().load(data=flask.request.json)

    elif flask.request.form:
      result = RegisterSchema().load(data=flask.request.form)

    else:
      raise remote_exceptions.BadRequest(f'Invalid Content-Type', {}, 400)

    if result.errors:
      raise remote_exceptions.BadRequest(f'Unable to register microservice', result.errors, 400)

    class TokenAuth(requests.auth.AuthBase):
      def __call__(self, request: typing.Any) -> 'Request':
        request.headers['Authorization'] = f'Bearer {result.data["nonce"]}'
        return request

    response = requests.post(SERVICE_CONNECTIONS[service_name]['shake_url'], data=json.dumps(SERVICE_CONNECTIONS[service_name]), headers={
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    }, auth=TokenAuth())
    if not response.status_code in [200]:
      if isinstance(response.content, bytes):
        content: str = response.content.decode(constants.ENCODING)
      else:
        content: str = response.content

      raise remote_exceptions.BadRequest(f'Microservice is Offline', {'errors': [{'url': SERVICE_CONNECTIONS[service_name]['shake_url'], 'content': content, 'status': response.status_code}]}, 422)

    return flask.Response('{}', content_type='application/json', status=200)

  return SERVICE_CONNECTIONS[service_name]['auth_token']

def handle_bad_request(exception: remote_exceptions.BadRequest) -> flask.Response:
  return exception.as_response()

