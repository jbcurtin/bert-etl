import logging
import os

from bert import exceptions as bert_exceptions

LOG_ERROR_ONLY: bool = False if os.environ.get('LOG_ERROR_ONLY', 'true') in ['f', 'false', 'no'] else True
try:
    MAX_RETRY: int = int(os.environ.get('MAX_RETRY', 10))
except ValueError:
    raise bert_exceptions.BertException(f'Unable to encode ENVVar:MAX_RETRY to int')

logger = logging.getLogger(__name__)
logger.info(f'Log Error Only[{LOG_ERROR_ONLY}]')
logger.info(f'Max Retry[{MAX_RETRY}]')

