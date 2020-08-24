#!/usr/env/bin python

import argparse
import functools
import logging
import multiprocessing
import os
import sys
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

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
STOP_DAEMON: bool = False
LOG_ERROR_ONLY = not bert_constants.DEBUG

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

def _select_previous_job(current_job_name: str, jobs: typing.Dict[str, typing.Any]) -> typing.Tuple['job_name', 'job_conf']:
    try:
        previous_job_name = [key for key in jobs.keys()][list(jobs.keys()).index(current_job_name) - 1]

    except IndexError:
        previous_job_name = None
        previous_job_conf = None

    else:
        previous_job_conf = jobs[previous_job_name]

    return previous_job_name, previous_job_conf

def handle_job_cache__begin__okay(options: argparse.Namespace, current_job_name: str, jobs: typing.Dict[str, typing.Any]) -> bool:
    if options.enable_job_cache is False:
        return True

    if options.enable_job_cache and options.start_before_job == current_job_name:
        previous_job_name, previous_job_conf = _select_previous_job(current_job_name, jobs)
        if previous_job_name is None or previous_job_conf['job'].cache_backend is None:
            return False

        cache_count = previous_job_conf['job'].cache_backend.done_queue_cache_size()
        logger.info(f'Filling Job[{current_job_name}] Work Queue from Cache. Cache Count[{cache_count}]')
        previous_job_conf['job'].cache_backend.clear_done_queue()
        previous_job_conf['job'].cache_backend.fill_done_queue_from_cache(options.queue_fill_count)
        return True

    if options.enable_job_cache and options.start_before_job:
        start_before_job_idx = list(jobs.keys()).index(options.start_before_job)
        current_job_idx = list(jobs.keys()).index(current_job_name)
        return current_job_idx > start_before_job_idx

    if options.enable_job_cache and not options.stop_after_job is None:
        return True

    return False

def handle_job_cache__done__cleanup(options: argparse.Namespace, job_name: str, conf: typing.Dict[str, typing.Any]) -> None:
    if options.enable_job_cache and conf['job'].cache_backend and options.stop_after_job == job_name:
        queue_size = conf['job'].cache_backend.done_queue_size()
        logger.info(f'Filling Job[{job_name}] Cache from Done Queue. Count[{queue_size}]')
        conf['job'].cache_backend.clear_cache_for_done_queue()
        conf['job'].cache_backend.fill_cache_from_done_queue()
        logger.info(f'Exiting After Job[{job_name}]')
        sys.exit(0)

def run_jobs(options: argparse.Namespace, jobs: typing.Dict[str, types.FunctionType]):
    if bert_constants.DEBUG:
        for idx, (job_name, conf) in enumerate(jobs.items()):

            if handle_job_cache__begin__okay(options, job_name, conf) is False:
                continue

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

            handle_job_cache__done__cleanup(options, job_name, conf)

    else:
        for idx, (job_name, conf) in enumerate(jobs.items()):
            if handle_job_cache__begin__okay(options, job_name, jobs) is False:
                continue

            bert_encoders.clear_encoding()
            bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])

            def print_begin_log_info(options: argparse.Namespace, job_name: str, conf: typing.Dict[str, typing.Any]) -> None:
                work_queue, done_queue, ologger = bert_utils.comm_binders(conf['job'])
                work_unit_count = work_queue.size()
                pipeline_type = conf['job'].pipeline_type.value
                logger.info(f'Running Job[{job_name}] - {pipeline_type} - Work Unit Count[{work_unit_count}]')
                logger.info(f'Job worker count[{conf["job"].workers}]')

            print_begin_log_info(options, job_name, conf)
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
                with bert_datasource.ENVVars({'BERT_MULTIPROCESSING': 't'}):
                    bert_encoders.clear_encoding()
                    bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])
                    bert_encoders.load_queue_encoders(conf['encoding']['queue_encoders'])
                    bert_encoders.load_queue_decoders(conf['encoding']['queue_decoders'])
                    execution_role_arn: str = conf['iam'].get('execution-role-arn', None)
                    job_restart_count: int = 0
                    job_work_queue, job_done_queue, ologger = bert_utils.comm_binders(conf['job'])
                    while job_restart_count < conf['runner']['max-retries']:
                        try:
                            if execution_role_arn is None:
                                with bert_datasource.ENVVars(conf['runner']['environment']):
                                    while True:
                                        conf['job']()
                                        time.sleep(bert_constants.LONG_DELAY)
                                        if job_work_queue.size() > 0:
                                            continue

                                        break

                                    sys.exit(0)
                            else:
                                with bert_aws.assume_role(execution_role_arn):
                                    with bert_datasource.ENVVars(conf['runner']['environment']):
                                        while True:
                                            conf['job']()
                                            time.sleep(bert_constants.LONG_DELAY)
                                            if job_work_queue.size() > 0:
                                                continue

                                            break

                                        sys.exit(0)

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
                proc: multiprocessing.Process = multiprocessing.Process(target=_job_runner, args=())
                proc.daemon = True
                proc.start()
                processes.append(proc)

            else:
                active_job_count = len([proc for proc in processes if proc.is_alive()])
                last_pulse = datetime.utcnow()
                while not STOP_DAEMON and any([proc.is_alive() for proc in processes]):
                    count = len([proc for proc in processes if proc.is_alive()])
                    if count != active_job_count:
                        active_job_count = count
                        logger.info(f'Active Job Count[{count}]')

                    pulse_diff = datetime.utcnow() - last_pulse
                    if pulse_diff > timedelta(seconds=bert_constants.SUPER_LONG_DELAY):
                        logger.info(f'Active Job Count[{count}]')
                        last_pulse = datetime.utcnow()

                        job_work_queue, job_done_queue, ologger = bert_utils.comm_binders(conf['job'])
                        work_count = job_work_queue.size()
                        if work_count > 0:
                            logging.info(f'Work amount left[{work_count}]')

                        else:
                            logger.info('All work consumed')

                    time.sleep(bert_constants.DELAY)

            handle_job_cache__done__cleanup(options, job_name, conf)

            bert_encoders.clear_encoding()
            bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])
            bert_encoders.load_queue_encoders(conf['encoding']['queue_encoders'])

def validate_options(options: argparse.Namespace, jobs: typing.Dict[str, typing.Any]) -> None:
    if options.enable_job_cache:
        job_names = [key for key in jobs.keys()]
        start_before_jobs = []
        stop_after_jobs = []
        for idx, (job_name, conf) in enumerate(jobs.items()):
            if conf['job'].cache_backend:
                stop_after_jobs.append(job_name)
                try:
                    start_before_jobs.append(job_names[idx + 1])
                except IndexError:
                    pass

        start_before_jobs_formatted = ', '.join(start_before_jobs)
        stop_after_job_formatted = ', '.join(stop_after_jobs)
        if options.start_before_job is None and options.stop_after_job is None:
            raise NotImplementedError('For Job Cache, you must specify either a Job to cache')

        if options.start_before_job and not options.start_before_job in start_before_jobs:
            raise NotImplementedError(f'Job does not have a cache_backend. Available Jobs: [{start_before_jobs_formatted}]')

        if options.stop_after_job and not options.stop_after_job in stop_after_jobs:
            raise NotImplementedError(f'Job does not have a cache_backend. Available Jobs: [{stop_after_job_formatted}]')

    if not options.enable_job_cache:
        if options.start_before_job or options.stop_after_job:
            raise NotImplementedError('This option is not available without turining one Job Caching. --enable-job-cache')

