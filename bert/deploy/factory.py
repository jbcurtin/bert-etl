#!/usr/env/bin python

import argparse
import enum
import functools
import importlib
import inspect
import logging
import multiprocessing
import json
import os
import signal
import time
import typing
import types

from datetime import datetime

from bert import utils as bert_utils, constants as bert_constants
from bert.deploy import utils as bert_deploy_utils

logger = logging.getLogger(__name__)

STOP_DAEMON: bool = False
class Service(enum.Enum):
    AWSLambda: str = 'aws-lambda'

def capture_options() -> typing.Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--service', default=Service.AWSLambda, type=Service)
    parser.add_argument('-u', '--undeploy', action="store_true", default=False)
    parser.add_argument('-i', '--invoke', action="store_true", default=False)
    parser.add_argument('-a', '--invoke-async', default=False, action='store_true')
    parser.add_argument('-m', '--module-name', default='bert')
    parser.add_argument('-f', '--flush', default=False, action="store_true")
    parser.add_argument('-d', '--dry-run', default=False, action="store_true", help="Create the lambda functions and output ./lambdas without deploying to AWS")
    return parser.parse_args()

def deploy_service(options) -> None:
    jobs: typing.Dict[str, typing.Any] = bert_utils.scan_jobs(options)
    jobs: typing.Dict[str, typing.Any] = bert_utils.map_jobs(jobs)

    if options.service == Service.AWSLambda:
        if options.invoke:
            import boto3
            job_name: str = [job for job in jobs.keys()][0]
            invoke_args: typing.List[typing.Dict[str, typing.Any]] = {key: value for key, value in jobs.items()}[job_name]['aws-deploy']['invoke-args']
            client = boto3.client('lambda')
            if len(invoke_args) < 1:
                logger.info(f'Invoking Job[{job_name}]')
                client.invoke(FunctionName=job_name, InvocationType='Event')

            else:
                logger.info(f'Invoking Job[{job_name}] with {len(invoke_args)} payloads.')
                if options.invoke_async:
                    for args in invoke_args:
                        payload: bytes = json.dumps({'bert-inputs': [args]}).encode(bert_constants.ENCODING)
                        client.invoke(FunctionName=job_name, InvocationType='Event', Payload=payload)
                else:
                    payload: bytes = json.dumps({'bert-inputs': invoke_args}).encode(bert_constants.ENCODING)
                    client.invoke(FunctionName=job_name, InvocationType='Event', Payload=payload)

            import sys; sys.exit(0)

        bert_deploy_utils.validate_inputs(jobs)
        bert_deploy_utils.build_project(jobs)
        bert_deploy_utils.build_lambda_handlers(jobs)
        bert_deploy_utils.build_archives(jobs)
        bert_deploy_utils.create_roles(jobs)
        bert_deploy_utils.scan_dynamodb_tables(jobs)
        bert_deploy_utils.destroy_lambda_to_table_bindings(jobs)
        bert_deploy_utils.destroy_lambda_concurrency(jobs)
        bert_deploy_utils.destroy_sns_topic_lambdas(jobs)
        bert_deploy_utils.destroy_lambdas(jobs)
        bert_deploy_utils.create_lambdas(jobs)
        bert_deploy_utils.create_lambda_concurrency(jobs)
        bert_deploy_utils.create_dynamodb_tables(jobs)
        bert_deploy_utils.create_reporting_dynamodb_table()
        bert_deploy_utils.bind_lambdas_to_tables(jobs)
        bert_deploy_utils.bind_events_for_bottle_functions(jobs)
        bert_deploy_utils.bind_events_for_init_function(jobs)

    else:
        raise NotImplementedError(options.service)

def run_from_cli():
    import sys, os
    sys.path.append(os.getcwd())
    options = capture_options()
    deploy_service(options)

if __name__ in ['__main__']:
    run_from_cli()

