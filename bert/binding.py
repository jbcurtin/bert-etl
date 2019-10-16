import functools
import hashlib
import logging
import marshmallow
# import marshmallow.schema.UnmarshalResult
import multiprocessing
import os
import types
import typing

from bert import constants, naming

DAISY_CHAIN = {}
REGISTRY: typing.Dict[str, types.FunctionType] = {}
ENCODING = 'utf-8'
NOOP_SPACE: str = naming.calc_func_space(constants.NOOP)
logger = logging.getLogger(__name__)

def _merge_parent_funcs(parent_func: types.FunctionType) -> typing.List[types.FunctionType]:
    if isinstance(parent_func, str):
        return []

    elif isinstance(parent_func, types.FunctionType):
        parents: typing.List[types.FunctionType] = getattr(parent_func, 'parent_funcs', [])[:]
        parents.append(parent_func)
        return parents

    else:
        raise NotImplementedError

# Bert, a microframework for simple ETL solution that helps
def follow(
  parent_func: typing.Union[str, types.FunctionType],
  pipeline_type: constants.PipelineType = constants.PipelineType.BOTTLE,
  workers: int = multiprocessing.cpu_count(),
  schema: marshmallow.Schema = None):

  parent_func_space = naming.calc_func_space(parent_func)
  parent_func_work_key = naming.calc_func_key(parent_func_space, 'work')
  parent_func_done_key = naming.calc_func_key(parent_func_space, 'done')
  if isinstance(parent_func, str) and parent_func_space == NOOP_SPACE:
    pass

  elif isinstance(parent_func, types.FunctionType):
    if getattr(parent_func, 'func_space', None) is None:
        parent_func.func_space = parent_func_space

    if getattr(parent_func, 'work_key', None) is None:
        parent_func.work_key = parent_func_work_key

    if getattr(parent_func, 'done_key', None) is None:
        parent_func.done_key = parent_func_done_key

  elif isinstance(parent_func, str):
    raise NotImplementedError(f'Follow Parent[{parent_func}] is not valid. Must be types.FunctionType')

  else:
    import ipdb; ipdb.set_trace()
    raise NotImplementedError

  @functools.wraps(parent_func)
  def _parent_wrapper(wrapped_func):
    # Let the parent know who follows
    wrapped_func.parent_func: typing.Union[str, types.FunctionType] = parent_func
    wrapped_func_space: str = naming.calc_func_space(wrapped_func)
    wrapped_func_work_key: str = parent_func_done_key
    wrapped_func_done_key: str = naming.calc_func_key(wrapped_func_space, 'done')
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

    if getattr(wrapped_func, 'schema', None) is None:
      wrapped_func.schema = schema

    if getattr(wrapped_func, 'parent_space', None) is None:
      if parent_func_space != NOOP_SPACE:
        wrapped_func.parent_func = parent_func
        wrapped_func.parent_space = parent_func_space
        wrapped_func.parent_func_work_key = parent_func.work_key
        wrapped_func.parent_func_done_key = parent_func.done_key
        wrapped_func.parent_noop_space = False
        wrapped_func.parent_funcs = _merge_parent_funcs(parent_func)

      else:
        wrapped_func.parent_space = None
        wrapped_func.parent_func = 'noop'
        wrapped_func.parent_func_work_key = None
        wrapped_func.parent_func_done_key = None
        wrapped_func.parent_noop_space = True
        wrapped_func.parent_funcs = []

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

