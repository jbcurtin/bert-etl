#!/usr/env/bin python

import enum
import logging
import multiprocessing
import os
import typing

from urllib.parse import urlparse

DNS: str = '8.8.8.8'
ENCODING: str = 'utf-8'
WORKERS: int = int(os.environ.get('WORKERS', 2))
XSD_STRUCTURES: typing.Dict[str, typing.Any] = {}
CACHE_DIR: str = '/tmp/etl-cache'
DEBUG = False if os.environ.get('DEBUG', 'true') in ['f', 'false', 'no'] else True
EVENT_LOOP: typing.Any = None
WIKI_DUMP_DIR: str = os.environ.get('DUMP_DIR', 'wiki-dump')
REDIS_HOST: str = 'localhost'
REDIS_PORT: int = 6379
REDIS_DB: int = 6
DATETIME_FORMAT: str = '%Y-%m-%dT%H:%M:%SZ'
OUTPUT_DIR: str = './wiki-dump'
DUMP_BASE_URL = 'https://dumps.wikimedia.org'
HEADERS = {}
DELAY: int = 5
OSI_KEY: str = os.environ.get('ISO_KEY', 'en')
DOCKER_NO_CACHE: bool = True if os.environ.get('DOCKER_NO_CACHE', 'false') in ['t', 'true', 'yes'] else False
DUMP_DIR: str = os.environ.get('DUMP_DIR', f'{OSI_KEY}wiki-dump')
SQL_DIR: str = os.environ.get('SQL_DIR', f'{OSI_KEY}wiki-sql')
PG_DATA: str = os.environ.get('PGDATA', f'{OSI_KEY}-pgdata')

NOOP: str = 'noop'
REDIS_URL: str = os.environ.get('REDIS_URL', 'http://localhost:6379/4')
_redis_parts: typing.Any = urlparse(REDIS_URL)
REDIS_DB = _redis_parts.path.strip('/')
REDIS_HOST, REDIS_PORT = _redis_parts.netloc.split(':')
REDIS_PORT: int = int(REDIS_PORT)
class PipelineType(enum.Enum):
  BOTTLE: str = 'Bottle'
  CONCURRENT: str = 'Concurrent'

# workers: int = 4
# work_queue: Queue = Queue('work-one')
# work_queue.flushdb()
# processes: typing.List[multiprocessing.Process] = []
# options: typing.Dict[str, typing.Any] = {}
# start: str = os.path.join(os.getcwd(), WIKI_DIRECTORY)

logger = logging.getLogger(__name__)
logger.info(f'DEBUG[{DEBUG}]')
logger.info(f'Docker No Cache[{DOCKER_NO_CACHE}]')

