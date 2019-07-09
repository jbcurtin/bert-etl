import collections
import getpass
import os
import redis
import typing

from urllib.parse import ParseResult, urlparse

PWN = typing.TypeVar('PWN')

class Postgres(collections.namedtuple('Postgres', ['host', 'port', 'dbname', 'username', 'password'])):
  __slots__ = ()
  @classmethod
  def ParseURL(cls: PWN, url: str) -> PWN:
    parts: ParseResult = urlparse(url)
    try:
      creds, rest = parts.netloc.split('@', 1)
    except ValueError:
      username = getpass.getuser()
      password = ''
      host, port = parts.netloc.split(':', 1)
    else:
      host, port = rest.split(':', 1)
      username, password = creds.split(':', 1)

    return cls(
      host=host,
      port=int(port),
      dbname=parts.path.strip('/'),
      username=username,
      password=password)

  def __enter__(self) -> PWN:
    os.environ['PGHOST'] = self.host
    os.environ['PGPORT'] = str(self.port)
    os.environ['PGDATABASE'] = self.dbname
    os.environ['PGUSER'] = self.username
    os.environ['PGPASSWORD'] = self.password

  def __exit__(self, type, value, traceback) -> None:
    del os.environ['PGHOST']
    del os.environ['PGPORT']
    del os.environ['PGDATABASE']
    del os.environ['PGUSER']
    del os.environ['PGPASSWORD']

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

