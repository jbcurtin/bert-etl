import logging
import os
import sys

logger = logging.getLogger(__name__)
from bert import constants, remote_webservice
logger.info(f'Starting service[{constants.SERVICE_NAME}] Daemon. Debug[{constants.DEBUG}]')
logger.info(f'Loading Service Module[{constants.SERVICE_MODULE}]')
if constants.SERVICE_MODULE is None:
  raise NotImplementedError(f'Missing ENVVar[SERVICE_MODULE]')

remote_webservice.load_service_module(constants.SERVICE_MODULE)
MIDDLEWARE = remote_webservice.setup_service()
