import functools
import hashlib
import logging
import multiprocessing
import os
import types
import typing

from bert import constants

DAISY_CHAIN = {}
REGISTRY: typing.Dict[str, types.FunctionType] = {}
ENCODING = 'utf-8'
NOOP_SPACE: str = hashlib.sha1(constants.NOOP.encode(ENCODING)).hexdigest()
logger = logging.getLogger(__name__)

# Bert, a microframework for simple ETL solution that helps
def follow(parent_func: typing.Union[str, types.FunctionType], pipeline_type: constants.PipelineType = constants.PipelineType.BOTTLE, workers: int = multiprocessing.cpu_count()):
  if isinstance(parent_func, str):
    parent_func_space = hashlib.sha1(parent_func.encode(ENCODING)).hexdigest()
    parent_func_work_key = hashlib.sha1(''.join([parent_func_space, 'work']).encode(ENCODING)).hexdigest()
    parent_func_done_key = hashlib.sha1(''.join([parent_func_space, 'done']).encode(ENCODING)).hexdigest()
    if parent_func_space != NOOP_SPACE:
      raise NotImplementedError(f'Follow Parent[{parent_func}] is not valid. Must be types.FunctionType')

  elif isinstance(parent_func, types.FunctionType):
    parent_func_space = hashlib.sha1(parent_func.__name__.encode(ENCODING)).hexdigest()
    parent_func_work_key = hashlib.sha1(''.join([parent_func_space, 'work']).encode(ENCODING)).hexdigest()
    parent_func_done_key = hashlib.sha1(''.join([parent_func_space, 'done']).encode(ENCODING)).hexdigest()
    if getattr(parent_func, 'work_key', None) is None:
      parent_func.work_key = parent_func_work_key

    if getattr(parent_func, 'done_key', None) is None:
      parent_func.done_key = parent_func_done_key

    logger.debug(parent_func_space, 'parent-func', parent_func.__name__)
  else:
    raise NotImplementedError

  @functools.wraps(parent_func)
  def _parent_wrapper(wrapped_func):
    wrapped_func_space: str = hashlib.sha1(wrapped_func.__name__.encode(ENCODING)).hexdigest()
    wrapped_func_work_key: str = parent_func_done_key
    wrapped_func_done_key: str = hashlib.sha1(''.join([wrapped_func_space, 'done']).encode(ENCODING)).hexdigest()
    wrapped_func_build_dir: str = os.path.join('/tmp', wrapped_func_space, 'build')

    if getattr(wrapped_func, 'func_space', None) is None:
      wrapped_func.func_space = wrapped_func_space

    if getattr(wrapped_func, 'work_key', None) is None:
      wrapped_func.work_key = wrapped_func_work_key

    if getattr(wrapped_func, 'done_key', None) is None:
      wrapped_func.done_key = wrapped_func_done_key

    if getattr(wrapped_func, 'build_dir', None) is None:
      wrapped_func.build_dir = wrapped_func_build_dir

    if getattr(wrapped_func, 'pipeline_type', None) is None:
      wrapped_func.pipeline_type = pipeline_type

    if getattr(wrapped_func, 'workers', None) is None:
      wrapped_func.workers = workers if pipeline_type == constants.PipelineType.CONCURRENT else 1

    @functools.wraps(wrapped_func)
    def _wrapper(*args, **kwargs):
      return wrapped_func(*args, **kwargs)

    chain: typing.List[types.FunctionType] = DAISY_CHAIN.get(parent_func_space, [])
    chain.append(wrapped_func_space)
    if len(chain) > 1:
      raise NotImplementedError('One child to parent per program')

    DAISY_CHAIN[parent_func_space] = chain
    REGISTRY[wrapped_func_space] = _wrapper

    return _wrapper
  return _parent_wrapper

def build_job_chain() -> typing.Any:
  job_chain: typing.List[types.FunctionType] = []
  job_chain.append(DAISY_CHAIN[NOOP_SPACE][0])
  while True:
    latest_job = job_chain[-1:][0]
    for key, values in DAISY_CHAIN.items():
      if latest_job == key:
        job_chain.append(values[0])

    if len(DAISY_CHAIN.keys()) == len(job_chain):
      break

  for idx, job_space in enumerate(job_chain):
    job_chain[idx] = REGISTRY[job_space]

  return job_chain

