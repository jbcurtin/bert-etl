#!/usr/env/bin python

import argparse
import functools
import importlib
import logging
import multiprocessing
import os
import signal
import time
import typing
import types

from bert import utils as bert_utils

from datetime import datetime

logger = logging.getLogger(__name__)

STOP_DAEMON: bool = False
LOG_ERROR_ONLY: bool = False if os.environ.get('LOG_ERROR_ONLY', 'true') in ['f', 'false', 'no'] else True
MAX_RESTART: int = int(os.environ.get('MAX_RESTART_COUNT', 10))

def capture_options() -> typing.Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--module-name', required=True, help='https://bert-etl.readthedocs.io/en/latest/module_name.html'))
    parser.add_argument('-f', '--flush-db', action='store_true', default=False)
    parser.add_argument('-w', '--web-service', default=False, action='store_true')
    parser.add_argument('-d', '--web-service-daemon', default=False, action='store_true')
    return parser.parse_args()

def setup(options: argparse.Namespace) -> None:
    if options.flush_db:
        bert_utils.flush_db()

def handle_signal(sig, frame):
  if sig == 2:
    global STOP_DAEMON
    STOP_DAEMON = True
    import sys; sys.exit(0)

  else:
    logger.info(f'Unhandled Signal[{sig}]')

def start_webservice(options: 'Namespace') -> None:
  signal.signal(signal.SIGINT, handle_signal)
  from bert import binding, constants, utils, remote_webservice
  logger.info(f'Starting WebService[{constants.SERVICE_NAME}]. Debug[{constants.DEBUG}]')
  remote_webservice.setup_service()
  remote_webservice.run_service()

def start_daemon(options: 'Namespace') -> None:
  signal.signal(signal.SIGINT, handle_signal)
  DELAY: float = 0.1
  from bert import binding, constants, utils, remote_daemon
  logger.info(f'Starting service[{constants.SERVICE_NAME}] Daemon. Debug[{constants.DEBUG}]')
  remote_daemon.setup_service()
  remote_daemon.run_service()

def start_service(options: argparse.Namespace) -> None:
    if options.flush_db:
        bert_utils.flush_db()

    setup(options)
    jobs = bert_utils.scan_jobs(options)
    if options.web_service:
        start_webservice(options)

    elif options.web_service_daemon:
        start_daemon(options)

    else:
        raise NotImplementedError('Unable to start service')

def run_from_cli():
    import sys, os
    sys.path.append(os.getcwd())
    options = capture_options()
    start_service(options)

if __name__ in ['__main__']:
    run_from_cli()

