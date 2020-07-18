#!/usr/env/bin python

import argparse
import functools
import logging
import multiprocessing
import os
import time
import types
import typing

from bert import \
    utils as bert_utils, \
    constants as bert_constants, \
    encoders as bert_encoders, \
    datasource as bert_datasource, \
    aws as bert_aws

from bert.runner import \
    constants as runner_constants

from bert.webservice import handler

logger = logging.getLogger(__name__)
STOP_DAEMON: bool = False

def run_webservice(options: argparse.Namespace, jobs: typing.Dict[str, 'conf']) -> None:
    for job_name, conf in jobs.items():
        bert_encoders.clear_encoding()
        bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])
        bert_encoders.load_queue_encoders(conf['encoding']['queue_encoders'])
        bert_encoders.load_queue_decoders(conf['encoding']['queue_decoders'])
        execution_role_arn: str = conf['iam'].get('execution-role-arn', None)
        api: 'API' = getattr(conf['job'], '_api', None)
        if api is None:
            raise NotImplementedError(f'API missing from bert-etl file')

        if execution_role_arn is None:
            with bert_datasource.ENVVars(conf['runner']['environment']):
                handler.serve_handler(api, conf['job'], conf['api']['stage'])

        else:
            with bert_aws.assume_role(execution_role_arn):
                with bert_datasource.ENVVars(conf['runner']['environment']):
                    handler.serve_handler(api, conf['job'], conf['api']['stage'])

