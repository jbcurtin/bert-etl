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
  nonce: str = 'aoeu'
  url: str = 'http://localhost:7000/video-miner.register'
  if constants.SERVICE_NAME == constants.SERVICE_NAME_DEFAULT:
    raise InvalidServiceName

  remote_utils.RemoteConfig.Update({'nonce': nonce})
  response = requests.post(url, data=json.dumps({'nonce': nonce}), headers={
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

