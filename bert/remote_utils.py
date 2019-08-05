import collections
import flask
import hashlib
import marshmallow
import marshmallow.schema
import typing

from bert import remote_exceptions, constants, \
  utils as bert_utils, \
  datasource as bert_datasource

PWN: typing.TypeVar = typing.TypeVar('PWN')
DEFAULT_CONTENT_TYPE: str = 'application/octet-stream'

class RemoteConfig(collections.namedtuple('RemoteConfig', constants.REMOTE_CONFIG_KEYS)):
  @classmethod
  def Update(cls: PWN, updates: typing.Dict[str, str]) -> PWN:
    conn: bert_datasource.RedisConnection = bert_datasource.RedisConnection.ParseURL(constants.REDIS_URL)
    config: typing.Dict[str, str] = {}
    for key in constants.REMOTE_CONFIG_KEYS:
      config[key] = conn.client.hget(constants.REMOTE_CONFIG_SPACE, key) or None
      if config[key]:
        config[key] = config[key].decode(constants.ENCODING)

    for key in updates.keys():
      if not key in constants.REMOTE_CONFIG_KEYS:
        raise NotImplementedError(f'Key not allowed[{key}]')

      conn.client.hset(constants.REMOTE_CONFIG_SPACE, key, updates[key])
      config[key] = updates[key]

    return cls(**config)

  @classmethod
  def Obtain(cls: PWN) -> PWN:
    conn: bert_datasource.RedisConnection = bert_datasource.RedisConnection.ParseURL(constants.REDIS_URL)
    config: typing.Dict[str, str] = {}
    for key in constants.REMOTE_CONFIG_KEYS:
      config[key] = conn.client.hget(constants.REMOTE_CONFIG_SPACE, key) or None
      if config[key]:
        config[key] = config[key].decode(constants.ENCODING)

    return cls(**config)


class GetData(collections.namedtuple('GetData', ['data'])):
  @classmethod
  def Obtain(cls: PWN) -> PWN:
    return cls(data={key: value for key, value in flask.request.args.items() if value})

class PostData(collections.namedtuple('PostData', ['data'])):
  @classmethod
  def Obtain(cls: PWN, schema: marshmallow.Schema = None) -> PWN:
    if flask.request.headers.get('Content-Type', DEFAULT_CONTENT_TYPE) in [
      'application/x-www-form-urlencoded',
      'application/x-www-form-urlencoded; charset=UTF-8',
      'application/x-www-form-urlencoded; charset=utf-8']:
      data: typing.Dict[str, typing.Any] = {key: value for key, value in flask.request.form.items()}

    elif flask.request.headers.get('Content-Type', DEFAULT_CONTENT_TYPE) in ['application/json']:
      data: typing.Dict[str, typing.Any] = {key: value for key, value in flask.request.json.items()}

    else:
      raise NotImplementedError(flask.request.headers.get('Content-Type', DEFAULT_CONTENT_TYPE))

    if schema is None:
      return cls(data=data)

    result: marshmallow.schema.UnmarshalResult = schema().load(data)
    if result.errors:
      raise remote_exceptions.BadRequest('Unable to parse input', result.errors, 400)

    return cls(data=data)

def build_auth_token(service_name: str, entropy: str) -> str:
  noop_space: str = hashlib.sha1(constants.NOOP.encode(constants.ENCODING)).hexdigest()
  route_auth: str = f'{service_name}{noop_space}{entropy}'
  return hashlib.sha256(route_auth.encode(constants.ENCODING)).hexdigest()

