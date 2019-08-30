#!/usr/env/bin python

import argparse
import enum
import functools
import importlib
import inspect
import logging
import multiprocessing
import os
import signal
import time
import typing
import types

from datetime import datetime

from bert_deploy import utils as bert_deploy_utils
from bert import utils as bert_utils

logger = logging.getLogger(__name__)

STOP_DAEMON: bool = False
class Service(enum.Enum):
    AWSLambda: str = 'aws-lambda'

def capture_options() -> typing.Any:
  parser = argparse.ArgumentParser()
  parser.add_argument('-s', '--service', default=Service.AWSLambda, type=Service)
  parser.add_argument('-m', '--module-name', default='bert')
  return parser.parse_args()

def deploy_service(options) -> None:
    jobs: typing.Dict[str, typing.Any] = bert_utils.scan_jobs(options)

    if options.service == Service.AWSLambda:
        lambdas: typing.Dict[str, typing.Any] = bert_deploy_utils.build_lambda_archives(jobs)
        bert_deploy_utils.build_dynamodb_tables(lambdas)
        bert_deploy_utils.create_lambda_roles(lambdas)
        bert_deploy_utils.destory_lambda_to_table_bindings(lambdas)
        bert_deploy_utils.upload_lambdas(lambdas)
        bert_deploy_utils.bind_lambdas_to_tables(lambdas)

        import boto3
        client = boto3.client('lambda')
        conf = lambdas[[item for item in lambdas.keys()][0]]
        client.invoke(
                FunctionName=conf['aws-lambda']['FunctionName'],
                InvocationType='Event')
        import ipdb; ipdb.set_trace()
        pass

    else:
        raise NotImplementedError(options.service)

if __name__ in ['__main__']:
  options = capture_options()
  deploy_service(options)

