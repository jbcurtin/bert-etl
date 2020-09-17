#!/usr/env/bin python

import argparse
import logging
import os
import signal
import typing

from bert import utils as bert_utils
from bert.runner.datatypes import CognitoTrigger

logger = logging.getLogger(__name__)

STOP_DAEMON: bool = False
LOG_ERROR_ONLY: bool = False if os.environ.get('LOG_ERROR_ONLY', 'true') in ['f', 'false', 'no'] else True
MAX_RESTART: int = int(os.environ.get('MAX_RESTART_COUNT', 10))


def capture_options() -> typing.Any:
    parser = argparse.ArgumentParser()
    # Bert JOB API
    parser.add_argument('-m', '--module-name', required=True, help='https://bert-etl.readthedocs.io/en/latest/module_name.html')
    
    # Data DB API
    parser.add_argument('-f', '--flush-db', action='store_true', default=False)
    # parser.add_argument('-t', '--database-type', type=QueueBackendType, default=QueueBackendType.Redis)

    # Webservice API... Pending removal
    parser.add_argument('-w', '--web-service', action='store_true', default=False)

    # AWS Cognito API
    parser.add_argument('-g', '--cognito', action='store_true', default=False)
    parser.add_argument('-t', '--cognito-trigger', type=CognitoTrigger, default=None)

    # Cache API
    parser.add_argument('-e', '--cache-enabled', action='store_true', default=False, help='Enable Cache API')

    # Replay API
    parser.add_argument('-r', '--replay-enabled', action='store_true', default=False, help='Enable Replay API')
    parser.add_argument('-n', '--replay-function-name', type=str, default=None, help='Which function to focus on?')
    parser.add_argument('-c', '--replay-fill-count', type=int, default=0, help='Fill Cache Count for Replay API?')
    parser.add_argument('-s', '--stop-after-function', action='store_true', default=False, help='Stop after replaying function?')
    return parser.parse_args()


def handle_signal(sig, frame):
  if sig == 2:
    global STOP_DAEMON
    STOP_DAEMON = True
    import sys; sys.exit(0)

  else:
    logger.info(f'Unhandled Signal[{sig}]')

def start_webservice(options: argparse.Namespace) -> None:
    if options.flush_db:
        bert_utils.flush_db()

    jobs = bert_utils.scan_jobs(options.module_name)
    jobs = bert_utils.map_jobs(jobs, options.module_name)

    signal.signal(signal.SIGINT, handle_signal)
    from bert.webservice import manager
    manager.validate_options(options, jobs)
    manager.run_webservice(options, jobs)

def test_cognito_event(options: argparse.Namespace) -> None:
    triggers = ','.join([member.value for member in CognitoTrigger])
    if options.cognito_trigger is None:
        raise Exception(f'Cognito Trigger type required: {triggers}')

    if options.flush_db:
        bert_utils.flush_db()

    jobs = bert_utils.scan_jobs(options.module_name)
    jobs = bert_utils.map_jobs(jobs, options.module_name)

    signal.signal(signal.SIGINT, handle_signal)
    from bert.runner import manager
    manager.validate_options(options, jobs)
    manager.run_jobs(options, jobs)

def start_service(options: argparse.Namespace) -> None:
    if options.flush_db:
        bert_utils.flush_db()

    jobs = bert_utils.scan_jobs(options.module_name)
    jobs = bert_utils.map_jobs(jobs, options.module_name)

    signal.signal(signal.SIGINT, handle_signal)
    from bert.runner import manager
    manager.validate_options(options, jobs)
    manager.run_jobs(options, jobs)

def run_from_cli():
    import sys, os
    sys.path.append(os.getcwd())
    options = capture_options()
    if options.web_service:
        start_webservice(options)

    elif options.cognito:
        test_cognito_event(options)

    else:
        start_service(options)

if __name__ in ['__main__']:
    run_from_cli()
