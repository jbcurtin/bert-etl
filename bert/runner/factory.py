#!/usr/env/bin python

import argparse
import logging
import os
import signal
import typing

from bert import utils as bert_utils

logger = logging.getLogger(__name__)

STOP_DAEMON: bool = False
LOG_ERROR_ONLY: bool = False if os.environ.get('LOG_ERROR_ONLY', 'true') in ['f', 'false', 'no'] else True
MAX_RESTART: int = int(os.environ.get('MAX_RESTART_COUNT', 10))


def capture_options() -> typing.Any:
    parser = argparse.ArgumentParser()
    # parser.add_argument('-n', '--new-module', default=None, required=False)
    parser.add_argument('-m', '--module-name', required=True, help='https://bert-etl.readthedocs.io/en/latest/module_name.html')
    parser.add_argument('-f', '--flush-db', action='store_true', default=False)
    return parser.parse_args()


def handle_signal(sig, frame):
  if sig == 2:
    global STOP_DAEMON
    STOP_DAEMON = True
    import sys; sys.exit(0)

  else:
    logger.info(f'Unhandled Signal[{sig}]')


def start_service(options: argparse.Namespace) -> None:
    if options.flush_db:
        bert_utils.flush_db()

    jobs = bert_utils.scan_jobs(options)
    jobs = bert_utils.map_jobs(jobs)

    signal.signal(signal.SIGINT, handle_signal)
    from bert.runner import manager
    manager.run_jobs(options, jobs)

def run_from_cli():
    import sys, os
    sys.path.append(os.getcwd())
    options = capture_options()
    start_service(options)

if __name__ in ['__main__']:
    run_from_cli()
