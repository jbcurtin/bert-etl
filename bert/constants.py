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
DEBUG: bool = False if os.environ.get('DEBUG', 'true').lower() in ['f', 'false', 'no'] else True
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

# Cloud vars
USE_DYNAMODB: bool = True if os.environ.get('USE_DYNAMODB', 'false').lower() in ['t', 'yes', 'true'] else False 

class PipelineType(enum.Enum):
  BOTTLE: str = 'Bottle'
  CONCURRENT: str = 'Concurrent'

class QueueTypes(enum.Enum):
    Dynamodb: str = 'dynamodb'
    # Used to invoke asynchronous lambdas
    StreamingQueue: str = 'streaming-queue'
    Redis: str = 'redis'
    LocalQueue: str = 'local-queue'

QueueType: str = os.environ.get('BERT_QUEUE_TYPE', 'redis')
if QueueType.lower() in ['dynamodb']:
    QueueType = QueueTypes.Dynamodb

elif QueueType.lower() in ['local-queue']:
    QueueType = QueueType.LocalQueue

elif QueueType.lower() in ['streaming-queue']:
    QueueType = QueueTypes.StreamingQueue

elif QueueType.lower() in ['redis']:
    QueueType = QueueTypes.Redis

else:
    raise NotImplementedError(f'QueueType not found[{QueueType}]')

if os.environ.get('AWS_EXECUTION_ENV', None) is None:
    AWS_LAMBDA_FUNCTION: bool = False
else:
    AWS_LAMBDA_FUNCTION: bool = True

logger = logging.getLogger(__name__)
logger.info(f'DEBUG[{DEBUG}]')

