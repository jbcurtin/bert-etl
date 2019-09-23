#!/usr/env/bin python

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
    datasource as bert_datasource

from bert.runner import \
    constants as runner_constants

logger = logging.getLogger(__name__)
STOP_DAEMON: bool = False

def run_jobs(options: 'argparse.Options', jobs: typing.Dict[str, types.FunctionType]):
    if bert_constants.DEBUG:
        for job_name, conf in jobs.items():
            bert_encoders.clear_encoding()
            bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])
            bert_encoders.load_queue_encoders(conf['encoding']['queue_encoders'])
            bert_encoders.load_queue_decoders(conf['encoding']['queue_decoders'])
            logger.info(f'Running Job[{job_name}] as [{conf["spaces"]["pipeline-type"]}]')
            with bert_datasource.ENVVars(conf['runner']['environment']):
                conf['job']()

    else:
        for job_name, conf in jobs.items():
            logger.info(f'Running Job[{conf["job"].func_space}] as [{conf["job"].pipeline_type.value}] for [{conf["job"].__name__}]')
            logger.info(f'Job worker count[{conf["job"].workers}]')
            processes: typing.List[multiprocessing.Process] = []

            @functools.wraps(conf['job'])
            def _job_runner(*args, **kwargs) -> None:
                bert_encoders.clear_encoding()
                bert_encoders.load_identity_encoders(conf['encoding']['identity_encoders'])
                bert_encoders.load_queue_encoders(conf['encoding']['queue_encoders'])
                bert_encoders.load_queue_decoders(conf['encoding']['queue_decoders'])
                job_restart_count: int = 0
                while job_restart_count < conf['runner']['max-retries']:
                    try:

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

