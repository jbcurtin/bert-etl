import json
import logging
import requests
import time

from bert import remote_utils, constants, binding, utils, remote_callback

DELAY: float = .1
STOP_DAEMON: bool = False
logger = logging.getLogger(__name__)

def handle_signal(sig, frame):
  if sig == 2:
    global STOP_DAEMON
    STOP_DAEMON = True
    import sys; sys.exit(0)

  else:
    logger.info(f'Unhandled Signal[{sig}]')

def setup_service() -> None:
  if constants.SERVICE_NAME == None:
    raise NotImplementedError('Missing ENVVar[SERVICE_NAME]')

  if constants.MAIN_SERVICE_NONCE is None:
    raise NotImplementedError(f'Missing ENVVar[MAIN_SERVICE_NONCE]')

  if constants.MAIN_SERVICE_HOST is None:
    raise NotImplementedError(f'Missing ENVVar[MAIN_SERVICE_HOST]')

  remote_utils.RemoteConfig.Update({'nonce': constants.MAIN_SERVICE_NONCE})
  url: str = f'{constants.MAIN_SERVICE_HOST}/{constants.SERVICE_NAME}.register'
  response = requests.post(url, data=json.dumps({'nonce': constants.MAIN_SERVICE_NONCE}), headers={
    'Accept': 'application/json',
    'Content-Type': 'application/json',
  })
  response.raise_for_status()


def run_service():
  if not constants.DEBUG:
    raise NotImplementedError

  if constants.DEBUG:
    job_chain: typing.List[types.FunctionType] = binding.build_job_chain()
    noop_queue: utils.Queue = utils.Queue(binding.NOOP_SPACE)
    job_queue: utils.Queue = utils.Queue(job_chain[0].work_key)

    while STOP_DAEMON is False:
      try:
        details: typing.Dict[str, typing.Any] = next(noop_queue)
      except StopIteration:
        time.sleep(DELAY)

      else:
        job_queue.put(details)

        for job in job_chain:
          logger.info(f'Running Job[{job.func_space}] as [{job.pipeline_type.value}] for [{job.__name__}]')
          job()

        else:
          tail_queue: utils.Queue = utils.Queue(job.done_key)
          for details in tail_queue:
            remote_callback.submit(constants.SERVICE_NAME, details)

