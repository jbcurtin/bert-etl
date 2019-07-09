#!/usr/env/bin python

import enum
import logging
import os
import typing

from urllib.parse import urlparse

NOOP: str = 'noop'
DNS: str = '8.8.8.8'
ENCODING: str = 'utf-8'
DEBUG: bool = False if os.environ.get('DEBUG', 'true') in ['f', 'false', 'no'] else True
DELAY: int = .1

REDIS_URL: str = os.environ.get('REDIS_URL', 'http://localhost:6379/4')

class PipelineType(enum.Enum):
  BOTTLE: str = 'Bottle'
  CONCURRENT: str = 'Concurrent'

logger = logging.getLogger(__name__)
logger.info(f'DEBUG[{DEBUG}]')

DOCKER_SERVICE_NAME: str = 'bert-etl-redis'
DOCKER_REDIS_IMAGE: str = 'library/redis:latest'
