import collections
import importlib
import logging
import multiprocessing
import os
import subprocess
import time
import types
import typing

from bert import \
    queues as bert_queues, \
    constants as bert_constants, \
    shortcuts as bert_shortcuts, \
    encoders as bert_encoders, \
    exceptions as bert_exceptions

from bert import naming as bert_naming

ZIP_EXCLUDES: typing.List[str] = [
    '*.exe', '*.DS_Store', '*.Python', '*.git', '.git/*', '*.zip', '*.tar.gz',
    '*.hg', 'pip', 'docutils*', 'setuputils*', '__pycache__/*',
]
COMMON_EXCLUDES: typing.List[str] = ['env', 'lambdas']

logger = logging.getLogger(__name__)

def scan_jobs(options) -> typing.Dict[str, typing.Any]:
    jobs: typing.Dict[str, typing.Any] = {}
    module = importlib.import_module(f'{options.module_name}.jobs')
    for member_name in dir(module):
        if member_name.startswith('_'):
            continue

        member = getattr(module, member_name)
        if type(member) != types.FunctionType:
            continue

        if not hasattr(member, 'done_key') or not hasattr(member, 'work_key'):
            continue

        jobs[member_name] = member

    # Order the jobs correctly
    ordered = collections.OrderedDict()
    while len(ordered.keys()) != len(jobs.keys()):
        if len(ordered.keys()) == 0:
            for job_name, job in jobs.items():
                if job.parent_func == 'noop':
                    ordered[job_name] = job
                    break

            else:
                raise NotImplementedError(f'NoopSpace not found')

        else:
            latest: types.FunctionType = [item for item in ordered.values()][-1]
            for job_name, job in jobs.items():
                if job.parent_func == latest:
                    ordered[job_name] = job
                    break
    return ordered

def map_jobs(jobs: typing.Dict[str, typing.Any]) -> None:
    confs: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    bert_configuration = bert_shortcuts.load_configuration() or {}
    deployment_config = bert_shortcuts.obtain_deployment_config(bert_configuration)

    # Validate we have access to objects in AWS
    bert_shortcuts.head_bucket_for_existance(deployment_config.s3_bucket)

    for job_name, job in jobs.items():
        identity_encoders: typing.List[str] = bert_shortcuts.merge_lists(
            bert_configuration.get('every_lambda', {'identity_encoders': []}).get('identity_encoders', []),
            bert_configuration.get(job_name, {'identity_encoders': []}).get('identity_encoders', []),
            ['bert.encoders.base.IdentityEncoder'])

        queue_encoders: typing.List[str] = bert_shortcuts.merge_lists(
            bert_configuration.get('every_lambda', {'queue_encoders': []}).get('queue_encoders', []),
            bert_configuration.get(job_name,        {'queue_encoders': []}).get('queue_encoders', []),
            ['bert.encoders.base.encode_aws_object'])

        queue_decoders: typing.List[str] = bert_shortcuts.merge_lists(
            bert_configuration.get('every_lambda', {'queue_decoders': []}).get('queue_decoders', []),
            bert_configuration.get(job_name,        {'queue_decoders': []}).get('queue_decoders', []),
            ['bert.encoders.base.decode_aws_object'])

        # Make sure the encoders exist
        bert_encoders.load_encoders_or_decoders(identity_encoders)
        bert_encoders.load_encoders_or_decoders(queue_encoders)
        bert_encoders.load_encoders_or_decoders(queue_decoders)

        runtime: int = bert_shortcuts.get_if_exists('runtime', 'python3.6', str, bert_configuration.get('every_lambda', {}), bert_configuration.get(job_name, {}))
        memory_size: int = bert_shortcuts.get_if_exists('memory_size', '128', int, bert_configuration.get('every_lambda', {}), bert_configuration.get(job_name, {}))
        if int(memory_size / 64) != memory_size / 64:
            raise bert_exceptions.BertConfigError(f'MemorySize[{memory_size}] must be a multiple of 64')

        timeout: int = bert_shortcuts.get_if_exists('timeout', '900', int, bert_configuration.get('every_lambda', {}), bert_configuration.get(job_name, {}))
        env_vars: typing.Dict[str, str] = bert_shortcuts.merge_env_vars(
            bert_configuration.get('every_lambda', {'environment': {}}).get('environment', {}),
            bert_configuration.get(job_name, {'environment': {}}).get('environment', {}))

        # Set QueueType to dynamodb unless they've specifically requested a BERT_QUEUE_TYPE
        env_vars['BERT_QUEUE_TYPE'] = env_vars.get('BERT_QUEUE_TYPE', 'dynamodb')
        requirements: typing.Dict[str, str] = bert_shortcuts.merge_requirements(
            bert_configuration.get('every_lambda', {'requirements': {}}).get('requirements', {}),
            bert_configuration.get(job_name, {'requirements': {}}).get('requirements', {}))

        confs[job_name] = {
                'job': job,
                'deployment': {key: value for key, value in deployment_config._asdict().items()},
                'aws-deployed': {},
                'aws-deploy': {
                    'timeout': timeout,
                    'runtime': runtime,
                    'memory-size': memory_size, # must be a multiple of 64, increasing memory size also increases cpu allocation
                    'requirements': requirements,
                    'handler': f'{bert_naming.calc_lambda_name(job_name)}.{job_name}_handler',
                    'lambda-name': f'{bert_naming.calc_lambda_name(job_name)}',
                    'work-table-name': f'{bert_naming.calc_table_name(job.work_key)}',
                    'done-table-name': f'{bert_naming.calc_table_name(job.done_key)}',
                    'environment': env_vars,
                },
                'aws-build': {
                    'lambdas-path': os.path.join(os.getcwd(), 'lambdas'),
                    'excludes': ZIP_EXCLUDES + COMMON_EXCLUDES,
                    'path': os.path.join(os.getcwd(), 'lambdas', job_name),
                    'archive-path': os.path.join(os.getcwd(), 'lambdas', f'{job_name}.zip')
                },
                'runner': {
                    'environment': env_vars,
                    'max-retries': 10,
                },
                'spaces': {
                    'func_space': job.func_space,
                    'work-key': job.work_key,
                    'done-key': job.done_key,
                    'pipeline-type': job.pipeline_type,
                    'workers': job.workers,
                    'scheme': job.schema,
                },
                'encoding': {
                    'identity_encoders': identity_encoders,
                    'queue_encoders': queue_encoders,
                    'queue_decoders': queue_decoders,
                }
            }

    return confs

def run_command(cmd: str, allow_error: typing.List[int] = [0]) -> str:
    ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
    cmd: typing.List[str] = cmd.split(' ')
    proc = subprocess.Popen(' '.join(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    while proc.poll() is None:
        time.sleep(.1)

    if proc.poll() > 0:
        if not proc.poll() in allow_error:
            raise NotImplementedError(f'{proc.poll()}, {proc.stderr.read()}')

        return proc.stderr.read().decode(bert_constants.ENCODING)

    return proc.stdout.read().decode(bert_constants.ENCODING)


def comm_binders(func: types.FunctionType) -> typing.Tuple['QueueType', 'QueueType', 'ologger']:
    logger.info(f'Bert Queue Type[{bert_constants.QueueType}]')
    ologger = logging.getLogger('.'.join([func.__name__, multiprocessing.current_process().name]))
    if bert_constants.QueueType is bert_constants.QueueTypes.Dynamodb:
        return bert_queues.DynamodbQueue(func.work_key), bert_queues.DynamodbQueue(func.done_key), ologger

    elif bert_constants.QueueType is bert_constants.QueueTypes.StreamingQueue:
        return bert_queues.StreamingQueue(func.work_key), bert_queues.StreamingQueue(func.done_key), ologger

    elif bert_constants.QueueType is bert_constants.QueueTypes.LocalQueue:
        return bert_queues.LocalQueue(func.work_key), bert_queues.LocalQueue(func.done_key), ologger

    elif bert_constants.QueueType is bert_constants.QueueTypes.Redis:
        return bert_queues.RedisQueue(func.work_key), bert_queues.RedisQueue(func.done_key), ologger

    else:
        raise NotImplementedError(f'Unsupported QueueType[{bert_constants.QueueType}]')


def flush_db():
    if constants.QueueType in [
        constants.QueueType.Dynamodb,
        constants.QueueType.StreamingQueue]:
        raise NotImplementedError(f'Flush Dynamodb Tables')

    elif constants.QueueType is constants.QueueType.Redis:
        import redis
        from bert import constants, datasource
        redis_connection: datasource.RedisConnection = datasource.RedisConnection.ParseURL(constants.REDIS_URL)
        logger.info(f'Flushing Redis DB[{redis_connection.db}]')
        redis_connection.client.flushdb()

    else:
        raise NotImplementedError(constants.QueueType)


