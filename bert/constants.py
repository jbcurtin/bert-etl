#!/usr/env/bin python

import enum
import hashlib
import logging
import os
import typing

from urllib.parse import urlparse

NOOP: str = 'noop'
DNS: str = '8.8.8.8'
ENCODING: str = 'utf-8'
DEBUG: bool = False if os.environ.get('DEBUG', 'true') in ['f', 'false', 'no'] else True
DELAY: int = .1
DATETIME_FORMAT: str = '%Y-%m-%dT%H:%M:%SZ'
WWW_SECRET: str = os.environ.get('WWW_SECRET', 'noop')
WWW_PORT: int = int(os.environ.get('WWW_PORT', 8000))

MAIN_SERVICE_HOST: str = os.environ.get('MAIN_SERVICE_HOST', None)
MAIN_SERVICE_NONCE: str = os.environ.get('MAIN_SERVICE_NONCE', None)

SERVICE_HOST: str = os.environ.get('SERVICE_HOST', None)
SERVICE_NAME: str = os.environ.get('SERVICE_NAME', None)
SERVICE_MODULE: str = os.environ.get('SERVICE_MODULE', None)

REDIS_URL: str = os.environ.get('REDIS_URL', 'http://localhost:6379/4')
if SERVICE_NAME:
  REMOTE_CONFIG_SPACE: str = ''.join([SERVICE_NAME, WWW_SECRET])
  REMOTE_CONFIG_SPACE: str = hashlib.sha256(REMOTE_CONFIG_SPACE.encode(ENCODING)).hexdigest()
  REMOTE_CONFIG_KEYS: str = ['nonce', 'auth_token', 'callback_url']

else:
  REMOTE_CONFIG_SPACE: str = None
  REMOTE_CONFIG_SPACE: str = 'remote-config-space'
  REMOTE_CONFIG_KEYS: str = []

class PipelineType(enum.Enum):
  BOTTLE: str = 'Bottle'
  CONCURRENT: str = 'Concurrent'

logger = logging.getLogger(__name__)
logger.info(f'DEBUG[{DEBUG}]')

