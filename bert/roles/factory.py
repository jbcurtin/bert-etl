#!/usr/env/bin python

import argparse
import enum
import logging
import os
import signal
import types
import typing

from bert import \
    utils as bert_utils, \
    shortcuts as bert_shortcuts, \
    exceptions as bert_exceptions, \
    aws as bert_aws

logger = logging.getLogger(__name__)

STOP_DAEMON: bool = False
LOG_ERROR_ONLY: bool = False if os.environ.get('LOG_ERROR_ONLY', 'true') in ['f', 'false', 'no'] else True
MAX_RESTART: int = int(os.environ.get('MAX_RESTART_COUNT', 10))

class Role(enum.Enum):
    BertEtlAdmin: str = 'admin-type'
    BertEtlOperator: str = 'operator-type'
    BertEtlExecution: str = 'execution-type'

def capture_options() -> typing.Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--role-name', required=True, type=str)
    parser.add_argument('-r', '--role-type', required=True, type=Role)
    parser.add_argument('-a', '--authorize-user', default=False, action='store_true')
    parser.add_argument('-o', '--dry-run-off', default=False, action='store_true')
    parser.add_argument('-u', '--update-key-policy', default=False, action='store_true')
    return parser.parse_args()


def run_roles(options: argparse.Namespace) -> None:
    bert_configuration = bert_shortcuts.load_configuration() or {}
    raise NotImplementedError

def run_from_cli():
    import sys, os
    sys.path.append(os.getcwd())
    options = capture_options()
    run_roles(options)

if __name__ in ['__main__']:
    run_from_cli()

