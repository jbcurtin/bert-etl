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
    constants as runner_constants, \
    datatypes as runner_datatypes

logger = logging.getLogger(__name__)
STOP_DAEMON: bool = False

def inject_cognito_event(conf: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    event_defaults = runner_datatypes.CognitoEventDefaults(
        clientId = conf['aws-deploy']['cognito']['client_id'],
        userPoolId = conf['aws-deploy']['cognito']['user_pool_id'])

    triggers = [member for member in runner_datatypes.CognitoTrigger if member.value in conf['aws-deploy']['cognito']['triggers']]
    if len(triggers) < 0:
        raise NotImplementedError(f'Cognito Event not found in bert-etl file.')

    if len(triggers) > 1:
        raise NotImplementedError(f'Cognito Events >1 not supported yet')

    event = runner_datatypes.CognitoEvent(event_defaults)
    return event.trigger_content(triggers[0])

def run_jobs(options: argparse.Namespace, jobs: typing.Dict[str, types.FunctionType]):
    if bert_constants.DEBUG:
        for idx, (job_name, conf) in enumerate(jobs.items()):
            if options.stop_at_job and options.stop_at_job == job_name:
                logger.info(f'Stoping at Job[{job_name}]')
                break

            if options.jump_to_job and options.jump_to_job != job_name:
                logger.info(f'Skipping Job[{job_name}]')
                continue

            if options.jump_to_job and options.jump_to_job == job_name:
                previous_job_name = [k for k in jobs.keys()][idx - 1]
                previous_job_conf = jobs[previous_job_name]
                cache_backend = previous_job_conf['job'].cache_backend
                if cache_backend:
                    cache_backend.clear_queue(conf['job'].work_key)
                    cache_backend.fill_queue(conf['job'].work_key, options.jump_to_number)

            bert_encoders.clear_encoding()
            bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])
            bert_encoders.load_queue_encoders(conf['encoding']['queue_encoders'])
            bert_encoders.load_queue_decoders(conf['encoding']['queue_decoders'])
            logger.info(f'Running Job[{job_name}] as [{conf["spaces"]["pipeline-type"]}]')
            execution_role_arn: str = conf['iam'].get('execution-role-arn', None)
            job_worker_queue, job_done_queue, job_logger = bert_utils.comm_binders(conf['job'])

            if options.cognito is True:
                job_worker_queue.put({
                    'cognito-event': inject_cognito_event(conf)
                })

            for invoke_arg in conf['aws-deploy']['invoke-args']:
                job_worker_queue.put(invoke_arg)

            if execution_role_arn is None:
                with bert_datasource.ENVVars(conf['runner']['environment']):
                    conf['job']()

            else:
                with bert_aws.assume_role(execution_role_arn):
                    with bert_datasource.ENVVars(conf['runner']['environment']):
                        conf['job']()

    else:
        for job_name, conf in jobs.items():
            # Todo: Add jump-to-job here
            if options.stop_at_job and options.stop_at_job == job_name:
                logger.info(f'Stoping at Job[{job_name}]')
                break

            logger.info(f'Running Job[{conf["job"].func_space}] as [{conf["job"].pipeline_type.value}] for [{conf["job"].__name__}]')
            logger.info(f'Job worker count[{conf["job"].workers}]')
            processes: typing.List[multiprocessing.Process] = []
            bert_encoders.clear_encoding()
            bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])
            bert_encoders.load_queue_encoders(conf['encoding']['queue_encoders'])
            bert_encoders.load_queue_decoders(conf['encoding']['queue_decoders'])

            job_worker_queue, job_done_queue, job_logger = bert_utils.comm_binders(conf['job'])
            if options.cognito is True:
                job_worker_queue.put({
                    'cognito-event': inject_cognito_event(conf)
                })

            for invoke_arg in conf['aws-deploy']['invoke-args']:
                job_worker_queue.put(invoke_arg)

            @functools.wraps(conf['job'])
            def _job_runner(*args, **kwargs) -> None:
                bert_encoders.clear_encoding()
                bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])
                bert_encoders.load_queue_encoders(conf['encoding']['queue_encoders'])
                bert_encoders.load_queue_decoders(conf['encoding']['queue_decoders'])
                execution_role_arn: str = conf['iam'].get('execution-role-arn', None)
                job_restart_count: int = 0
                while job_restart_count < conf['runner']['max-retries']:
                    try:

                        if execution_role_arn is None:
                            with bert_datasource.ENVVars(conf['runner']['environment']):
                                conf['job']()

                        else:
                            with bert_aws.assume_role(execution_role_arn):
                                with bert_datasource.ENVVars(conf['runner']['environment']):
                                    conf['job']()

                    except Exception as err:
                        if LOG_ERROR_ONLY:
                            logger.exception(err)

                        else:
                            raise err
                    else:
                        break

                    job_restart_count += 1
                else:
                    logger.exception(f'Job[{conf["job"].func_space}] failed {job_restart_count} times')

            for idx in range(0, conf['job'].workers):
                logging.info(f'Spawning Process[{idx}]')
                proc: multiprocessing.Process = multiprocessing.Process(target=_job_runner, args=())
                proc.daemon = True
                proc.start()
                processes.append(proc)

            else:
                while not STOP_DAEMON and any([proc.is_alive() for proc in processes]):
                    time.sleep(bert_constants.DELAY)

