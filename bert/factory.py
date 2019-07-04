#!/usr/env/bin python

import argparse
import docker
import functools
import importlib
import logging
import multiprocessing
import os
import signal
import time
import typing
import types


from bert import constants, utils

from datetime import datetime

from docker.errors import APIError

logger = logging.getLogger(__name__)

JOBS: typing.Dict[str, types.FunctionType] = {}
STOP_DAEMON: bool = False

def capture_options() -> typing.Any:
  parser = argparse.ArgumentParser()
  parser.add_argument('-n', '--new-module', default=None, required=False)
  parser.add_argument('-j', '--jobs', default=None, required=False)
  parser.add_argument('-d', '--debug', action='store_false', default=True)
  parser.add_argument('-m', '--module-name', default='bert')
  parser.add_argument('-o', '--how', default=False, action='store_true')
  parser.add_argument('-f', '--flush-db', action='store_true', default=False)
  parser.add_argument('-c', '--channel', default=0, type=int, help="Redis DB[0-16]; Default 0")
  parser.add_argument('-r', '--redis-port', default=6379, type=int)
  parser.add_argument('-l', '--log-error-only', default=True, action="store_false")
  parser.add_argument('-e', '--restart-job', default=True, action="store_false")
  parser.add_argument('-a', '--max-restart', default=10, type=int)
  return parser.parse_args()

def setup(options: argparse.Namespace) -> None:
  from bert import constants
  docker_client: typing.Any = docker.from_env()
  if not constants.DOCKER_SERVICE_NAME in [c.name for c in docker_client.containers.list()]:
    logger.info('Starting RedisClient')
    try:
      docker_client.containers.run('library/redis:latest', name=constants.DOCKER_SERVICE_NAME, detach=True, ports={
      6379: options.redis_port
    })
    except APIError as err:
      logger.info("Redis is already running under another service-name. Let's use that.")

  if options.flush_db:
    import redis
    redis_client = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=options.channel)
    logger.info(f'Flushing Redis DB[{options.channel}]')
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

def validate_jobs(options) -> None:
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

      @functools.wraps(job)
      def _job_runner(*args, **kwargs):
        job_restart_count: int = 0
        while job_restart_count < options.max_restart:
          try:
            job(*args, **kwargs)
          except Exception as err:
            if options.log_error_only:
              logger.exception(err)

            else:
              raise err

          job_restart_count += 1
        else:
          logger.exception(f'Job[{job}] failed {job_restart_count} times')

      for idx in range(0, job.workers):
        logging.info(f'Spawning Process[{idx}]')
        proc: multiprocessing.Process = multiprocessing.Process(target=_job_runner, args=())
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

