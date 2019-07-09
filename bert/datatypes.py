import collections
import redis
import typing

from urllib.parse import ParseResult, urlparse

PWN: typing.Any = typing.TypeVar('PWN')

class RedisConnection(collections.namedtuple('RedisConnection', ['host', 'port', 'db', 'client'])):
  __slots__ = ()
  @classmethod
  def ParseURL(cls: PWN, url: str) -> PWN:
    parts: ParseResult = urlparse(url)
    host, port = parts.netloc.split(':')
    return cls(
      host=host,
      port=int(port),
      db=parts.path.strip('/'),
      client=redis.Redis(host=host, port=int(port), db=parts.path.strip('/')))

