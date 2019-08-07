#!/usr/env/bin python

import argparse
import functools
import importlib
import logging
import multiprocessing
import os
import signal
import time
import typing
import types


from datetime import datetime

logger = logging.getLogger(__name__)

JOBS: typing.Dict[str, types.FunctionType] = {}
STOP_DAEMON: bool = False

def capture_options() -> typing.Any:
  parser = argparse.ArgumentParser()
  parser.add_argument('-n', '--new-module', default=None, required=False)
  parser.add_argument('-m', '--module-name', default='bert')
  parser.add_argument('-o', '--how', default=False, action='store_true')
  parser.add_argument('-f', '--flush-db', action='store_true', default=False)
  parser.add_argument('-l', '--log-error-only', default=True, action="store_false")
  parser.add_argument('-e', '--restart-job', default=True, action="store_false")
  parser.add_argument('-a', '--max-restart', default=10, type=int)
  parser.add_argument('-w', '--web-service', default=False, action='store_true')
  parser.add_argument('-d', '--web-service-daemon', default=False, action='store_true')
  return parser.parse_args()

def setup(options: argparse.Namespace) -> None:
  from bert import constants, datasource
  redis_connection: datasource.RedisConnection = datasource.RedisConnection.ParseURL(constants.REDIS_URL)

  if options.flush_db:
    import redis
    logger.info(f'Flushing Redis DB[{redis_connection.db}]')
    redis_connection.client.flushdb()

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
    logger.info(f'Unhandled Signal[{sig}]')

def start_webservice(options: 'Namespace') -> None:
  signal.signal(signal.SIGINT, handle_signal)
  from bert import binding, constants, utils, remote_webservice
  logger.info(f'Starting WebService[{constants.SERVICE_NAME}]. Debug[{constants.DEBUG}]')
  remote_webservice.setup_service()
  remote_webservice.run_service()

def start_daemon(options: 'Namespace') -> None:
  signal.signal(signal.SIGINT, handle_signal)
  DELAY: float = 0.1
  from bert import binding, constants, utils, remote_daemon
  logger.info(f'Starting service[{constants.SERVICE_NAME}] Daemon. Debug[{constants.DEBUG}]')
  remote_daemon.setup_service()
  remote_daemon.run_service()

def start_jobs(options):
  signal.signal(signal.SIGINT, handle_signal)
  from bert import binding, constants
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

          else:
            break

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

def start_service(options: argparse.Namespace) -> None:
  setup(options)
  scan_jobs(options)
  options = capture_options()
  setup(options)
  scan_jobs(options)
  # validate_jobs(options)
  if options.web_service:
    start_webservice(options)

  elif options.web_service_daemon:
    start_daemon(options)

  else:
    start_jobs(options)

if __name__ in ['__main__']:
  options = capture_options()
  start_service(options)

