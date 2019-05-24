#!/usr/env/bin python

import argparse
import importlib
import logging
import multiprocessing
import os
import signal
import time
import typing
import types

from datetime import datetime

from bert import constants, utils

logger = logging.getLogger(__name__)

JOBS: typing.Dict[str, types.FunctionType] = {}
STOP_DAEMON: bool = False

def capture_options() -> typing.Any:
  parser = argparse.ArgumentParser()
  parser.add_argument('-n', '--new-module', default=None, required=False)
  parser.add_argument('-j', '--jobs', default=None, required=True)
  parser.add_argument('-d', '--debug', action='store_false', default=True)
  parser.add_argument('-m', '--module-name', default='bert')
  parser.add_argument('-f', '--flush-db', action='store_true', default=False)
  return parser.parse_args()

def setup(options) -> None:
  from bert import constants
  if options.flush_db:
    import redis
    redis_client = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=constants.REDIS_DB)
    logger.info(f'Flushing Redis DB[{constants.REDIS_DB}]')
    redis_client.flushdb()
    
def scan_jobs(options):
  global JOBS
  module = importlib.import_module(f'{options.module_name}.jobs')
  for member_name in dir(module):
    if member_name.startswith('_'):
      continue

    member = getattr(module, member_name)
    if type(member) != types.FunctionType:
      continue

    JOBS[member_name] = member

def validate_jobs(options):
  for job in options.jobs.split(','):
    if job not in JOBS.keys():
      raise NotImplementedError(f'Job[{job}] not found. Available Jobs[{", ".join(JOBS.keys())}]')

def handle_signal(sig, frame):
  if sig == 2:
    global STOP_DAEMON
    STOP_DAEMON = True
    import sys; sys.exit(0)

  else:
    logger.info('Unhandled Signal[{sig}]')

def start_jobs(options):
  signal.signal(signal.SIGINT, handle_signal)
  from bert import binding
  if constants.DEBUG:
    job_chain: typing.List[types.FunctionType] = binding.build_job_chain()
    for job in job_chain:
      logger.info(f'Running Job[{job.func_space}] as [{job.pipeline_type.value}] for [{job.__name__}]')
      job()

  else:
    job_chain: typing.List[types.FunctionType] = binding.build_job_chain()
    for job in job_chain:
      logger.info(f'Running Job[{job.func_space}] as [{job.pipeline_type.value}] for [{job.__name__}]')
      logger.info(f'Job worker count[{job.workers}]')
      processes: typing.List[multiprocessing.Process] = []
      for idx in range(0, job.workers):
        logging.info(f'Spawning Process[{idx}]')
        proc: multiprocessing.Process = multiprocessing.Process(target=job, args=())
        proc.daemon = True
        proc.start()
        processes.append(proc)

      else:
        while not STOP_DAEMON and any([proc.is_alive() for proc in processes]):
          time.sleep(constants.DELAY)

if __name__ in ['__main__']:
  options = capture_options()
  setup(options)
  scan_jobs(options)
  validate_jobs(options)
  start_jobs(options)

