import logging

logger = logging.getLogger(__name__)

class BertSettings(collections.NamedTuple):
    requirements: typing.List[str]
    identity_encoders: typing.List[typing.Any]
    queue_encoders: typing.List[typing.Any]
    queue_decoders: typing.List[typing.Any]
    env_vars: typing.Dict[str, str]
    runtime: str
    memory_size: int
    timeout: int

CONFIGS: typing.Dict[str, BertSettings] = {}
def capture(job_name: str) -> BertSettings:
    bert_configuration = bert_deploy_shortcuts.load_local_configuration()
    import ipdb; ipdb.set_trace()
    pass

# def build_project_envs(jobs: typing.Dict[str, types.FunctionType], venv_path: str, excludes: typing.List[str] = COMMON_EXCLUDES) -> typing.Dict[str, typing.Any]:
#     confs: typing.Dict[str, typing.Any] = {}
# 
#     for job_name, job in jobs.items():
#         identity_encoders: typing.List[str] = bert_deploy_shortcuts.merge_lists(
#             bert_configuration.get('every_lambda', {'identity_encoders': []}).get('identity_encoders', []),
#             bert_configuration.get(job_name, {'identity_encoders': []}).get('identity_encoders', []),
#             ['bert.encoders.base.IdentityEncoder'])
# 
#         queue_encoders: typing.List[str] = bert_deploy_shortcuts.merge_lists(
#             bert_configuration.get('every_lambda', {'queue_encoders': []}).get('queue_encoders', []),
#             bert_configuration.get(job_name,        {'queue_encoders': []}).get('queue_encoders', []),
#             ['bert.encoders.base.encode_aws_object'])
# 
#         queue_decoders: typing.List[str] = bert_deploy_shortcuts.merge_lists(
#             bert_configuration.get('every_lambda', {'queue_decoders': []}).get('queue_decoders', []),
#             bert_configuration.get(job_name,        {'queue_decoders': []}).get('queue_decoders', []),
#             ['bert.encoders.base.decode_aws_object'])
# 
#         bert_encoders.load_encoders_or_decoders(identity_encoders)
#         bert_encoders.load_encoders_or_decoders(queue_encoders)
#         bert_encoders.load_encoders_or_decoders(queue_decoders)
# 
#         # Align the lists with expectations of top -> bottom execution in bert-etl.yaml
#         # identity_encoders.reverse()
#         # queue_encoders.reverse()
#         # queue_decoders.reverse()
# 
#         runtime: int = bert_deploy_shortcuts.get_if_exists('runtime', 'python3.6', str, bert_configuration.get('every_lambda', {}), bert_configuration.get(job_name, {}))
#         memory_size: int = bert_deploy_shortcuts.get_if_exists('memory_size', '128', int, bert_configuration.get('every_lambda', {}), bert_configuration.get(job_name, {}))
#         if int(memory_size / 64) != memory_size / 64:
#             raise deploy_exceptions.BertConfigError(f'MemorySize[{memory_size}] must be a multiple of 64')
# 
#         timeout: int = bert_deploy_shortcuts.get_if_exists('timeout', '900', int, bert_configuration.get('every_lambda', {}), bert_configuration.get(job_name, {}))
#         env_vars: typing.Dict[str, str] = bert_deploy_shortcuts.merge_env_vars(
#             bert_configuration.get('every_lambda', {'environment': {}}).get('environment', {}),
#             bert_configuration.get(job_name, {'environment': {}}).get('environment', {}))
# 
#         requirements: typing.Dict[str, str] = bert_deploy_shortcuts.merge_requirements(
#             bert_configuration.get('every_lambda', {'requirements': {}}).get('requirements', {}),
#             bert_configuration.get(job_name, {'requirements': {}}).get('requirements', {}))
# 
#         env_vars['BERT_QUEUE_TYPE'] = env_vars.get('BERT_QUEUE_TYPE', 'dynamodb')
#         project_path: str = tempfile.mkdtemp(prefix=f'bert-etl-project-{job_name}')
#         logger.info(f'Creating Project Path[{project_path}]')
#         job_templates: str = ''.join([f"""def {jn}():
#     pass
# """ for jn in jobs.keys() if jn != job_name])
# 
#         job_follow: str = inspect.getsource(job).split('\n')[0]
#         job_source: str = '\n'.join(inspect.getsource(job).split('\n')[2:])
#         job_template: str = """
# %s
# import typing
# 
# from bert import utils, constants, binding, shortcuts, encoders
# encoders.load_identity_encoders(%s)
# encoders.load_queue_encoders(%s)
# encoders.load_queue_decoders(%s)
# 
# %s
# def %s(event: typing.Dict[str, typing.Any] = {}, context: 'lambda_context' = None) -> None:
# 
#     records: typing.List[typing.Dict[str, typing.Any]] = event.get('Records', [])
#     if len(records) > 0 and constants.DEBUG == False:
#         constants.QueueType = constants.QueueTypes.StreamingQueue
#         work_queue, done_queue, ologger = utils.comm_binders(%s)
#         for record in records:
#             if record['eventName'].lower() == 'INSERT'.lower():
#                 work_queue.local_put(record['dynamodb']['NewImage'])
# 
#     elif constants.DEBUG:
#         constants.QueueType = constants.QueueTypes.LocalQueue
#         work_queue, done_queue, ologger = utils.comm_binders(%s)
#         work_queue.local_put(event)
# 
#     else:
#         work_queue, done_queue, ologger = utils.comm_binders(%s)
# 
#     ologger.info(f'QueueType[{constants.QueueType}]')
# 
# %s
#     return {}
# """ % (job_templates, identity_encoders, queue_encoders, queue_decoders, job_follow, job_name, job_name, job_name, job_name, job_source)
#         job_path: str = os.path.join(project_path, f'{job_name}.py')
# 
#         with open(job_path, 'w') as stream:
#             stream.write(job_template)
# 
#         logger.info(f'Creating Job[{job_name}] Project')
#         copytree(os.getcwd(), project_path, metadata=False, symlinks=False, ignore=shutil.ignore_patterns(*excludes))
#         logger.info(f'Merging Job[{job_name}] Site Packages')
#         copytree(venv_path, project_path, metadata=False, symlinks=False, ignore=shutil.ignore_patterns(*excludes))
#         logger.info(f'Merging Job[{job_name}] Requirements')
#         bert_utils.run_command(f'pip install -t {project_path} {" ".join(requirements)} -U')
#         confs[_calc_lambda_name(job_name)] = {
#                 'project-path': project_path,
#                 'table-name': f'{job_name}-stream',
#                 'timeout': timeout,
#                 'runtime': runtime,
#                 'memory-size': memory_size, # must be a multiple of 64, increasing memory size also increases cpu allocation
#                 'environment': env_vars,
#                 'requirements': requirements,
#                 'handler-name': f'{job_name}.{job_name}',
#                 'spaces': {
#                     'work-key': job.work_key,
#                     'done-key': job.done_key,
#                     'pipeline-type': job.pipeline_type,
#                     'workers': job.workers,
#                     'scheme': job.schema,
#                 },
#                 'encoding': {
#                     'identity_encoders': identity_encoders,
#                     'queue_encoders': queue_encoders,
#                     'queue_decoders': queue_decoders,
#                 }
#             }
# 
#     return confs
# 
# def build_package(job_name: str, job_conf: typing.Dict[str, typing.Any], excludes: typing.List[str] = COMMON_EXCLUDES) -> None:
#     try:
#         compression_method: int = zipfile.ZIP_DEFLATED
#     except ImportError: #pragma: no cover
#         compression_method: int = zipfile.ZIP_STORED
# 
#     archive_name: str = f'{job_name}.zip'
#     archive_dir: str = os.path.join(os.getcwd(), 'lambdas')
#     if not os.path.exists(archive_dir):
#         os.makedirs(archive_dir)
# 
#     archive_path: str = os.path.join(archive_dir, archive_name)
#     logger.info(f'Building Lambda[{job_name}] Archive[{archive_path}]')
# 
#     job_conf['archive-path'] = archive_path
#     with zipfile.ZipFile(archive_path, 'w', compression_method) as archive:
#         for root, dirs, files in os.walk(job_conf['project-path']):
#             for filename in files:
#                 if filename in excludes:
#                     continue
# 
#                 if filename.endswith('.pyc'):
#                     continue
# 
#                 abs_filename: str = os.path.join(root, filename)
#                 if filename.endswith('.py'):
#                     os.chmod(abs_filename, 0o755)
# 
#                 zip_info: zipfile.ZipInfo = zipfile.ZipInfo(os.path.join(root.replace(job_conf['project-path'], '').lstrip(os.sep), filename))
#                 zip_info.create_system = 3
#                 zip_info.external_attr = 0o755 << int(16)
#                 with open(abs_filename, 'rb') as file_stream:
#                     archive.writestr(zip_info, file_stream.read(), compression_method)
# 
#             for dirname in dirs:
#                 if dirname in excludes:
#                     continue
# 
# 
#     job_conf['archive-path'] = archive_path
# 
# def include_bert_dev(bert_dev_path: str, venv_path: str, excludes: typing.List[str] = []) -> None:
#     excludes = ZIP_EXCLUDES + excludes + ['lamdbas']
#     temp_bert_path: str = tempfile.mkdtemp(prefix='bert-dev-path')
#     bert_dev: str = os.path.join(temp_bert_path, 'bert')
#     # Make sure the correct filepath was provided
#     for filename in ['__init__.py', 'factory.py', 'binding.py', 'constants.py', 'utils.py', 'shortcuts.py']:
#         assert filename in os.listdir(bert_dev_path), f'Incorrect BERT_DEV[{bert_dev_path}] provided, filename[{filename}] not found'
# 
#     copytree(bert_dev_path, bert_dev, metadata=False, symlinks=False, ignore=shutil.ignore_patterns(*excludes))
#     copytree(temp_bert_path, venv_path, metadata=False, symlinks=False, ignore=shutil.ignore_patterns(*excludes))
# 
# 
# def build_lambda_archives(jobs: typing.Dict[str, types.FunctionType]) -> str:
#     venv_path: str = get_current_venv()
#     if venv_path is None:
#         venv_path = get_pyenv_venv()
#         if venv_path is None:
#             venv_path = get_conda_venv()
#             if venv_path is None:
#                 raise NotImplementedError
# 
#     if os.environ.get('BERT_DEV', None):
#         logger.warning('BERT_DEV ENVVar found, including development version of bert-etl')
#         include_bert_dev(os.environ['BERT_DEV'], venv_path)
# 
#     archive_paths: typing.List[str] = []
#     lambdas: typing.Dict[str, typing.Any] = collections.OrderedDict()
#     for job_name, job_conf in build_project_envs(jobs, venv_path).items():
#         build_package(job_name, job_conf)
#         lambdas[job_name] = job_conf
#         # shutil.rmtree(venv_path)
#         shutil.rmtree(job_conf['project-path'])
# 
#     return lambdas
# 
# # def replace_lambda_archives_with_requires(build_lambda_archives
# def destroy_dynamodb_tables(jobs: typing.Dict[str, typing.Any]) -> None:
#     table: typing.Any = None
#     client = boto3.client('dynamodb')
#     for job_name, job in jobs.items():
#         work_table_name: str = _calc_table_name(job.work_key)
#         done_table_name: str = _calc_table_name(job.done_key)
# 
#         try:
#             client.delete_table(TableName=work_table_name)
#         except ClientError as err:
#             pass
# 
#         else:
#             logger.info(f'Destorying Table[{work_table_name}]')
# 
#         try:
#             client.delete_table(TableName=done_table_name)
#         except ClientError as err:
#             pass
# 
#         else:
#             logger.info(f'Destorying Table[{done_table_name}]')
# 
# 
# def build_dynamodb_tables(lambdas: typing.Dict[str, typing.Any]) -> None:
#     table: typing.Any = None
#     client = boto3.client('dynamodb')
#     for lambda_name, conf in lambdas.items():
#         work_table_name: str = _calc_table_name(conf['spaces']['work-key'])
#         done_table_name: str = _calc_table_name(conf["spaces"]["done-key"])
# 
#         try:
#             conf['work-table'] = client.describe_table(TableName=work_table_name)
#         except ClientError as err:
#             logger.info(f'Creating Dynamodb Table[{work_table_name}]')
#             client.create_table(
#                     TableName=work_table_name,
#                     KeySchema=[
#                         {
#                             'AttributeName': 'identity',
#                             'KeyType': 'HASH'
#                         }
#                     ],
#                     AttributeDefinitions=[
#                         {
#                             'AttributeName': 'identity',
#                             'AttributeType': 'S'
#                         }
#                     ],
#                     StreamSpecification={
#                         'StreamEnabled': True,
#                         'StreamViewType': 'NEW_IMAGE'
#                     },
#                     BillingMode='PAY_PER_REQUEST')
#             conf['work-table'] = client.describe_table(TableName=work_table_name)
# 
#         try:
#             conf['done-table'] = client.describe_table(TableName=done_table_name)
#         except ClientError as err:
#             logger.info(f'Creating Dynamodb Table[{done_table_name}]')
#             client.create_table(
#                     TableName=done_table_name,
#                     KeySchema=[
#                         {
#                             'AttributeName': 'identity',
#                             'KeyType': 'HASH'
#                         }
#                     ],
#                     AttributeDefinitions=[
#                         {
#                             'AttributeName': 'identity',
#                             'AttributeType': 'S'
#                         }
#                     ],
#                     StreamSpecification={
#                         'StreamEnabled': True,
#                         'StreamViewType': 'NEW_IMAGE'
#                     },
#                     BillingMode='PAY_PER_REQUEST')
# 
#             conf['done-table'] = client.describe_table(TableName=done_table_name)
# 
# 
# def create_lambda_roles(lambdas: typing.Dict[str, typing.Any]) -> None:
#     trust_policy_document = {
#         "Version": "2012-10-17",
#         "Statement": [
#             {
#                 "Sid": "",
#                 "Effect": "Allow",
#                 "Principal": {
#                     "Service": [
#                         "apigateway.amazonaws.com",
#                         "lambda.amazonaws.com",
#                         "events.amazonaws.com",
#                         "dynamodb.amazonaws.com",
#                     ]
#                 },
#                 "Action": "sts:AssumeRole",
#             }
#         ]
#     }
# 
#     policy_document: typing.Dict[str, typing.Any] = {
#         "Version": "2012-10-17",
#         "Statement": [
#             # {
#             #     "Effect": "Allow",
#             #     "Action": [
#             #         "xray:PutTraceSegments",
#             #         "xray:PutTelemetryRecords",
#             #     ],
#             #     "Resource": ["*"],
#             # },
#             # {
#             #     "Effect": "Allow",
#             #     "Action": [
#             #         "lambda:InvokeFunction"
#             #     ],
#             #     "Resource": ["*"],
#             # },
#             {
#                 "Effect": "Allow",
#                 "Action": [
#                     "dynamodb:*",
#                 ],
#                 "Resource": "arn:aws:dynamodb:*:*:table/*",
#             },
#             {
#                 "Effect": "Allow",
#                 "Action": "s3:*",
#                 "Resource": "*"
#             },
#             {
#                 "Effect": "Allow",
#                 "Action": [
#                     "logs:CreateLogStream",
#                     "logs:PutLogEvents",
#                     "logs:CreateLogGroup",
#                 ],
#                 "Resource": "arn:aws:logs:*:*:*",
#             }
#         ]
#     }
#     trust_policy_name: str = 'bert-etl-lambda-execution-policy-trust'
#     policy_name: str = 'bert-etl-lambda-execution-policy'
#     iam_role = {
#         'Path': '/',
#         'RoleName': trust_policy_name,
#         'AssumeRolePolicyDocument': json.dumps(trust_policy_document),
#         'Description': 'Bert-ETL Lambda Execution Role',
#     }
#     iam_policy = {
#         'Path': '/',
#         'PolicyName': policy_name,
#         'PolicyDocument': json.dumps(policy_document),
#         'Description': 'Bert-ETL Lambda Execution Policy'
#     }
#     iam_client = boto3.client('iam')
#     role = bert_deploy_shortcuts.map_iam_role(trust_policy_name)
#     if role is None:
#         iam_client.create_role(**iam_role)
#         role = bert_deploy_shortcuts.map_iam_role(trust_policy_name)
# 
#     policy = bert_deploy_shortcuts.map_iam_policy(policy_name)
#     if policy is None:
#         iam_client.create_policy(**iam_policy)
#         policy = bert_deploy_shortcuts.map_iam_policy(policy_name)
#         iam_client.attach_role_policy(RoleName=trust_policy_name, PolicyArn=policy['Arn'])
# 
#     for job_name, conf in lambdas.items():
#         conf['iam-role'] = role
#         conf['iam-policy'] = policy
# 
# def destory_lambda_to_table_bindings(lambdas: typing.Dict[str, typing.Any]) -> None:
#     client = boto3.client('lambda')
#     for lambda_name, conf in lambdas.items():
#         for event_mapping in client.list_event_source_mappings(
#                 EventSourceArn=conf['work-table']['Table']['LatestStreamArn'],
#                 FunctionName=lambda_name)['EventSourceMappings']:
#             client.delete_event_source_mapping(UUID=event_mapping['UUID'])
# 
# def destroy_lambdas(jobs: typing.Dict[str, typing.Any]) -> None:
#     client = boto3.client('lambda')
#     for job_name, job in jobs.items():
#         lambda_name: str = _calc_lambda_name(job_name)
#         try:
#             client.delete_function(FunctionName=lambda_name)
#         except ClientError as err:
#             pass
# 
#         else:
#             logger.info(f'Deleting function[{lambda_name}]')
# 
# 
# def upload_lambdas(lambdas: typing.Dict[str, typing.Any]) -> None:
#     client = boto3.client('lambda')
#     for lambda_name, conf in lambdas.items():
#         try:
#             client.get_function(FunctionName=lambda_name)['Configuration']
#         except ClientError as err:
#             logger.info(f'Creating AWSLambda for Job[{lambda_name}]')
#             lambda_description = client.create_function(
#                 FunctionName=lambda_name,
#                 Runtime=conf['runtime'],
#                 MemorySize=conf['memory-size'],
#                 Role=conf['iam-role']['Arn'],
#                 Handler=conf['handler-name'],
#                 Code={
#                     'ZipFile': open(conf['archive-path'], 'rb').read(),
#                 },
#                 Timeout=conf['timeout'],
#                 Environment={'Variables': conf['environment']},
#             )
#             conf['aws-lambda'] = client.get_function(FunctionName=lambda_name)['Configuration']
# 
#         else:
#             logger.info(f'Replacing AWSLambda for Job[{lambda_name}]')
#             client.delete_function(FunctionName=lambda_name)
#             client.create_function(
#                 FunctionName=lambda_name,
#                 Runtime=conf['runtime'],
#                 MemorySize=conf['memory-size'],
#                 Role=conf['iam-role']['Arn'],
#                 Handler=conf['handler-name'],
#                 Code={
#                     'ZipFile': open(conf['archive-path'], 'rb').read(),
#                 },
#                 Timeout=conf['timeout'],
#                 Environment={'Variables': conf['environment']},
#             )
#             conf['aws-lambda'] = client.get_function(FunctionName=lambda_name)['Configuration']
# 
# def bind_lambdas_to_tables(lambdas: typing.Dict[str, typing.Any]) -> None:
#     client = boto3.client('lambda')
#     for lambda_name, conf in lambdas.items():
#         logger.info(f'Mapping Lambda[{lambda_name}] to Work-Table')
#         client.create_event_source_mapping(
#                 EventSourceArn=conf['work-table']['Table']['LatestStreamArn'],
#                 FunctionName=lambda_name,
#                 Enabled=True,
#                 StartingPosition='LATEST')
# 
