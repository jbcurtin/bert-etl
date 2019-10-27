import boto3
import collections
import glob
import logging
import inspect
import json
import os
import shutil
import tempfile
import time
import types
import typing
import zipfile

from bert import \
    utils as bert_utils, \
    encoders as bert_encoders, \
    exceptions as bert_exceptions, \
    constants as bert_constants

from bert.deploy import \
    shortcuts as bert_deploy_shortcuts, \
    exceptions as deploy_exceptions, \
    reporting as bert_reporting

from botocore.errorfactory import ClientError

from distutils.sysconfig import get_python_lib

# Related: https://github.com/Miserlou/Zappa/pull/56
# Related: https://github.com/Miserlou/Zappa/pull/581
ZIP_EXCLUDES: typing.List[str] = [
    '*.exe', '*.DS_Store', '*.Python', '*.git', '.git/*', '*.zip', '*.tar.gz',
    '*.hg', 'pip', 'docutils*', 'setuputils*', '__pycache__/*',
]
COMMON_EXCLUDES: typing.List[str] = ['env', 'lambdas']
try:
    COMPRESSION_METHOD: int = zipfile.ZIP_DEFLATED
except ImportError: #pragma: no cover
    COMPRESSION_METHOD: int = zipfile.ZIP_STORED


logger = logging.getLogger(__name__)

def _calc_lambda_name(lambda_name: str) -> str:
    return lambda_name

def _calc_table_name(lambda_key: str) -> str:
    return lambda_key

def copytree(src: str, dest: str, metadata: bool = True, symlinks: bool = False, ignore: typing.Any = None) -> None:
    if isinstance(ignore, (types.GeneratorType, list, set, tuple)):
        ignore = shutil.ignore_patterns(*ignore)

    if not os.path.exists(dest):
        os.makedirs(dest)
        if metadata:
            shutil.copystat(src, dest)

    dir_list: typing.List[str] = os.listdir(src)
    if ignore:
        excl = ignore(src, dir_list)
        dir_list = [item for item in dir_list if item not in excl]

    for item in dir_list:
        source_path: str = os.path.join(src, item)
        dest_path: str = os.path.join(dest, item)
        if symlinks and os.path.islink(source_path):
            if os.path.lexsits(dest_path):
                os.remove(dest_path)

            os.symlink(os.readlink(source_path), dest_path)
            if metadata:
                try:
                    stats: typing.Any = os.lstat(source_path)
                    mode = stat.S_IMODE(stats.st_mode)
                    os.lchmod(dest_path, mode)
                except:
                    pass # lchmod not availabe

        elif os.path.isdir(source_path):
            copytree(source_path, dest_path, metadata, symlinks, ignore)

        else:
            shutil.copy2(source_path, dest_path) if metadata else shutil.copy(source_path, dest_path)

def find_site_packages_dir(start: str, find: str) -> str:
    """
    Scan a directory[start] for a subdirectory[find]
    """
    if not os.path.isdir(start):
        raise IOError(f'Path[{start}] is not a directory.')

    for root, dirs, files in os.walk(start):
        if find in dirs:
            return os.path.join(start, find)

        for dir_name in dirs:
            result: str = find_site_packages_dir(os.path.join(start, dir_name), find)
            if result:
                return result


def build_lambda_handlers(jobs: typing.Dict[str, typing.Dict[str, typing.Any]]) -> None:
    for job_name, conf in jobs.items():
        job_source: str = inspect.getsource(conf['job']).split('\n')
        job_templates: typing.List[str] = []
        previous_job: str = None
        head_templates: typing.List[str] = []
        tail_templates: typing.List[str] = []
        head_on: bool = True
        for sub_name, sub_conf in jobs.items():
            if sub_name == job_name:
                previous_job = sub_name
                head_on = False
                continue

            if previous_job:
                template: str = '''
@binding.follow(%(parent_name)s)
def %(job_name)s() -> None:
    pass
'''% {
    'parent_name': previous_job,
    'job_name': sub_name
}
            else:
                template: str = '''
@binding.follow('noop')
def %(job_name)s() -> None:
    pass
''' % {
    'job_name': sub_name
}

            if head_on:
                head_templates.append(template)

            else:
                tail_templates.append(template)

            previous_job = sub_name
        else:
            head_templates: str = ''.join(head_templates)
            tail_templates: str = ''.join(tail_templates)

        source_code: str = """
import typing

from bert import utils, constants, binding, shortcuts, encoders
from bert import \
    utils as bert_utils, \
    constants as bert_constants, \
    binding, \
    shortcuts as bert_shortcuts, \
    encoders as bert_encoders
from bert.deploy import reporting as bert_reporting

bert_encoders.load_identity_encoders(%(identity_encoders)s)
bert_encoders.load_queue_encoders(%(queue_encoders)s)
bert_encoders.load_queue_decoders(%(queue_decoders)s)

%(head_templates)s

%(job_source)s

def %(job_name)s_handler(event: typing.Dict[str, typing.Any] = {}, context: 'lambda_context' = None) -> None:
    with bert_reporting.track_execution(%(job_name)s):
        print(event)
        records: typing.List[typing.Dict[str, typing.Any]] = event.get('Records', [])
        bert_inputs: typing.Dict[str, typing.Any] = event.get('bert-inputs', None)
        if len(records) > 0 and bert_constants.DEBUG == False:
            bert_constants.QueueType = bert_constants.QueueTypes.StreamingQueue
            work_queue, done_queue, ologger = bert_utils.comm_binders(%(job_name)s)
            for record in records:
                if record.get('EventSource', None) == 'aws:sns':
                    work_queue.local_put({'identity': {'S': 'sns-entry'}, 'datum': {'M': bert_encoders.encode_object(record['Sns'])}})

                elif record.get('eventSource', None) == 'aws:dynamodb':
                    if record['eventName'].lower() == 'INSERT'.lower():
                        work_queue.local_put(record['dynamodb']['NewImage'])

                else:
                    raise NotImplementedError(record['EventSource'])

        elif bert_inputs != None:
            bert_constants.QueueType = bert_constants.QueueTypes.StreamingQueue
            work_queue, done_queue, ologger = bert_utils.comm_binders(%(job_name)s)
            for record in bert_inputs:
                work_queue.local_put(record)

        elif bert_constants.DEBUG:
            bert_constants.QueueType = bert_constants.QueueTypes.LocalQueue
            work_queue, done_queue, ologger = bert_utils.comm_binders(%(job_name)s)
            work_queue.local_put(event)

        else:
            work_queue, done_queue, ologger = bert_utils.comm_binders(%(job_name)s)

        ologger.info(f'QueueType[{bert_constants.QueueType}]')
        %(job_name)s()


def %(job_name)s_manager(event: typing.Dict[str, typing.Any] = {}, context: 'lambda_context' = None) -> None:
    execution_manager = bert_reporting.manager(%(job_name)s)
    if execution_manager.is_safe():
        with bert_reporting.track_execution(%(job_name)s):
            work_queue, done_queue, ologger = bert_utils.comm_binders(%(job_name)s)
            ologger.info('Executing Bottle Function')
            ologger.info(f'QueueType[{bert_constants.QueueType}]')
            %(job_name)s()

%(tail_templates)s
""" % {
    'head_templates': head_templates,
    'tail_templates': tail_templates,
    'job_source': '\n'.join(job_source),
    'job_name': job_name,
    'identity_encoders': conf['encoding']['identity_encoders'],
    'identity_encoders': conf['encoding']['identity_encoders'],
    'queue_encoders': conf['encoding']['queue_encoders'],
    'queue_decoders': conf['encoding']['queue_decoders'],
}

        conf['lambda-path'] = os.path.join(conf['aws-build']['path'], f'{conf["job"].func_space}.py')
        logger.info(f'Creating Lambda Handler[{conf["lambda-path"]}]')
        with open(conf['lambda-path'], 'w') as stream:
            stream.write(source_code)


def build_package(job_name: str, job_conf: typing.Dict[str, typing.Any], excludes: typing.List[str] = COMMON_EXCLUDES) -> None:
    try:
        compression_method: int = zipfile.ZIP_DEFLATED
    except ImportError: #pragma: no cover
        compression_method: int = zipfile.ZIP_STORED

    archive_name: str = f'{job_name}.zip'
    archive_dir: str = os.path.join(os.getcwd(), 'lambdas')
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)

    archive_path: str = os.path.join(archive_dir, archive_name)
    logger.info(f'Building Lambda[{job_name}] Archive[{archive_path}]')

    job_conf['archive-path'] = archive_path
    with zipfile.ZipFile(archive_path, 'w', compression_method) as archive:
        for root, dirs, files in os.walk(job_conf['project-path']):
            for filename in files:
                if filename in excludes:
                    continue

                if filename.endswith('.pyc'):
                    continue

                abs_filename: str = os.path.join(root, filename)
                if filename.endswith('.py'):
                    os.chmod(abs_filename, 0o755)

                zip_info: zipfile.ZipInfo = zipfile.ZipInfo(os.path.join(root.replace(job_conf['project-path'], '').lstrip(os.sep), filename))
                zip_info.create_system = 3
                zip_info.external_attr = 0o755 << int(16)
                with open(abs_filename, 'rb') as file_stream:
                    archive.writestr(zip_info, file_stream.read(), compression_method)

            for dirname in dirs:
                if dirname in excludes:
                    continue


    job_conf['archive-path'] = archive_path

def include_bert_dev(dev_path: str, build_path: str, excludes: typing.List[str] = []) -> None:
    # Make sure the correct filepath was provided
    for filename in ['__init__.py', 'runner', 'deploy', 'remote', 'binding.py', 'constants.py', 'utils.py', 'shortcuts.py']:
        assert filename in os.listdir(dev_path), f'Incorrect BERT_DEV[{bert_dev_path}] provided, filename[{filename}] not found'

    bert_path: str = os.path.join(build_path, 'bert')
    if not os.path.exists(bert_path):
        os.makedirs(bert_path)

    logger.warning('BERT_DEV ENVVar found, including development version of bert-etl')
    copytree(dev_path, bert_path, metadata=False, symlinks=False, ignore=shutil.ignore_patterns(*excludes))


def _validate_concurrency(jobs: typing.Dict[str, typing.Any]) -> str:
    client = boto3.client('lambda')
    account_settings = client.get_account_settings()
    account_concurrency_limit = account_settings['AccountLimit']['ConcurrentExecutions']
    # Don't use UnreservedConcurrentExecutions because we'll release the ConcurrentExecutions with every deployment
    for job_name, conf in jobs.items():
        account_concurrency_limit = account_concurrency_limit - conf['aws-deploy']['concurrency-limit']
        if account_concurrency_limit < 100:

            message = f"""
The combination of functions uses more than {account_settings["AccountLimit"]["ConcurrentExecutions"]} executions.
AWS imposes this restruction so that some functions will always be able to execute.
Deployment Aborted
"""
            message += '\nJob Concurrency Settings: \n'
            for job_name, c_limit in [(job_name, conf['aws-deploy']['concurrency-limit']) for job_name, conf in jobs.items()]:
                message += f"{job_name}: {c_limit}\n"

            message += f'Only {account_settings["AccountLimit"]["ConcurrentExecutions"]} concurrent executions allowed in this AWS Account'
            message += '\nhttps://docs.aws.amazon.com/lambda/latest/dg/limits.html#limits'
            raise bert_exceptions.BertConfigError(message)

def _validate_schedule_expression_cron_grammar(schedule_expression: str, job_name: str) -> None:
    # raise NotImplementedError
    pass

def _validate_schedule_expression_rate_grammar(schedule_expression: str, job_name: str) -> None:
    try:
        count, interval = schedule_expression[5:-1].split(' ')
    except (IndexError, ValueError):
        raise bert_exceptions.BertConfigError(f'Unable to parse ScheduleExpression[{schedule_expression}] for job[{job_name}]')

    else:
        try:
            count = int(count)
        except ValueError:
            raise bert_exceptions.BertConfigError(f'Unable to parse ScheduleExpression[{schedule_expression}]. Unable to parse rate to int for job[{job_name}]')

        if count == 1 and interval != 'minute':
            raise bert_exceptions.BertConfigError(f'Unable to parse ScheduleExpression[{schedule_expression}]. With rate = 1, interval must be "minute" for job[{job_name}]')

        elif count > 1 and interval != 'minutes':
            raise bert_exceptions.BertConfigError(f'Unable to parse ScheduleExpression[{schedule_expression}]. With rate > 1, interval must be "minutes" for job[{job_name}]')

        elif count < 1:
            raise bert_exceptions.BertConfigError(f'Unable to parse ScheduleExpression[{schedule_expression}]. Rate must be > 0 for job[{job_name}]')


def _validate_schedule_expression(jobs: typing.Dict[str, typing.Any]) -> str:
    for job_name, conf in jobs.items():
        if conf['events']['schedule-expression'] is None:
            continue

        elif conf['events']['schedule-expression'].startswith('cron('):
            _validate_schedule_expression_cron_grammar(conf['events']['schedule-expression'], job_name)

        elif conf['events']['schedule-expression'].startswith('rate('):
            _validate_schedule_expression_rate_grammar(conf['events']['schedule-expression'], job_name)


def _validate_schedule_expression_bottle(jobs: typing.Dict[str, typing.Any]) -> str:
    for job_name, conf in jobs.items():
        if conf['spaces']['parent']['noop-space'] is False and conf['spaces']['pipeline-type'] == bert_constants.PipelineType.BOTTLE:
            if conf['bottle']['schedule-expression'] is None:
                raise bert_exceptions.BertConfigError(f'Unable to parse ScheduleExpression[{conf["bottle"]["schedule-expression"]}] for job[{job_name}].')

            elif conf['bottle']['schedule-expression'].startswith('cron('):
                _validate_schedule_expression_cron_grammar(conf['bottle']['schedule-expression'], job_name)

            elif conf['bottle']['schedule-expression'].startswith('rate('):
                _validate_schedule_expression_rate_grammar(conf['bottle']['schedule-expression'], job_name)


def validate_inputs(jobs: typing.Dict[str, typing.Any]) -> str:
    _validate_concurrency(jobs)
    _validate_schedule_expression(jobs)
    _validate_schedule_expression_bottle(jobs)

def build_project(jobs: typing.Dict[str, typing.Any]) -> str:
    for job_name, conf in jobs.items():
        src_dir: str = None
        site_package_dir: str = None
        if not os.path.exists(conf['aws-build']['lambdas-path']):
            os.makedirs(conf['aws-build']['lambdas-path'])

        if os.path.exists(conf['aws-build']['path']):
            shutil.rmtree(conf['aws-build']['path'])

        os.makedirs(conf['aws-build']['path'])

        # Resolve venv site-packages locations
        if 'VIRTUAL_ENV' in os.environ.keys():
            site_package_dir: str = find_site_packages_dir(os.environ['VIRTUAL_ENV'], 'site-packages')
            src_dir: str = os.path.join(os.environ['VIRTUAL_ENV'], 'src')

        # Resolve conda env site-packages location
        elif 'conda/envs' in get_python_lib() or 'miniconda/envs' in get_python_lib():
            site_package_dir: str = get_python_lib()

        else:
            site_package_dir: str = get_python_lib()

        if os.path.exists('.python-version'):
            import ipdb; ipdb.set_trace()
            raise NotImplementedError

        # Resolve Egg-Links
        # https://setuptools.readthedocs.io/en/latest/formats.html#egg-links
        egg_links: typing.List[str] = []
        for root, dirnames, filenames in os.walk(site_package_dir):
            for filename in filenames:
                if filename.endswith('.egg-link'):
                    egg_links.append(os.path.join(site_package_dir, filename))

        for filepath in egg_links:
            with open(filepath, 'r') as stream:
                data: str = stream.read()
                basename: str = os.path.basename(data)
                if not '\n' in data:
                    logger.error(f'error parsing egg-link for base[{basename}]. Please open an issue request with this error')

                dirpath: str = data.split('\n')[0]
                logger.info(f'Including DevPackage-EggLink[{basename}]')
                copytree(dirpath, conf['aws-build']['path'], metadata=False, symlinks=False, ignore=conf['aws-build']['excludes'])

        # Include Dev Version of bert-etl if found
        if os.environ.get('BERT_DEV', None):
            include_bert_dev(os.environ['BERT_DEV'], conf['aws-build']['path'], conf['aws-build']['excludes'])

        logger.info(f'Copying PWD[{os.getcwd()}] to Build Dir[{conf["aws-build"]["path"]}]')
        copytree(os.getcwd(), conf['aws-build']['path'], metadata=False, symlinks=False, ignore=conf['aws-build']['excludes'])
        logger.info(f'Copying SitePackages[{site_package_dir}] to Build Dir[{conf["aws-build"]["path"]}]')
        copytree(site_package_dir, conf['aws-build']['path'], metadata=False, symlinks=False, ignore=conf['aws-build']['excludes'])
        if src_dir and os.path.exists(src_dir):
            logger.info(f'Merging SrcDir[{src_dir}]')
            copytree(src_dir, conf['aws-build']['path'], metadata=False, symlinks=False, ignore=conf['aws-build']['excludes'])

        if conf['aws-deploy']['requirements']:
            logger.info(f'Merging Job[{job_name}] requirements')
            bert_utils.run_command(f'pip install -t {conf["aws-build"]["path"]} {" ".join(conf["aws-deploy"]["requirements"])} -U')

def build_archives(jobs: typing.Dict[str, typing.Any]) -> str:
    for job_name, conf in jobs.items():
        logger.info(f'Building Lambda Archive[{conf["aws-build"]["archive-path"]}]')
        with zipfile.ZipFile(conf['aws-build']['archive-path'], 'w', COMPRESSION_METHOD) as archive:
            for root, dirs, files in os.walk(conf['aws-build']['path']):
                for filename in files:
                    if filename in conf['aws-build']['excludes']:
                        continue

                    if filename.endswith('.pyc'):
                        continue

                    abs_filepath: str = os.path.join(root, filename)
                    if filename.endswith('.py'):
                        os.chmod(abs_filepath, 0o755)

                    relative_filepath = os.path.join(root.replace(conf['aws-build']['path'], '').lstrip(os.sep), filename)
                    zip_info: zipfile.ZipInfo = zipfile.ZipInfo(relative_filepath)
                    zip_info.create_system = 3
                    zip_info.external_attr = 0o755 << int(16)
                    with open(abs_filepath, 'rb') as file_stream:
                        archive.writestr(zip_info, file_stream.read(), COMPRESSION_METHOD)

                for dirname in dirs:
                    if dirname in conf['aws-build']['excludes']:
                        continue

        conf['aws-build']['archive-size'] = os.path.getsize(conf['aws-build']['archive-path'])


def scan_dynamodb_tables(jobs: typing.Dict[str, typing.Dict[str, typing.Any]]) -> None:
    client = boto3.client('dynamodb')
    for job_name, conf in jobs.items():
        try:
            conf['aws-deployed']['work-table'] = client.describe_table(TableName=conf['aws-deploy']['work-table-name'])
        except ClientError as err:
            conf['aws-deployed']['work-table'] = None

        try:
            conf['aws-deployed']['done-table'] = client.describe_table(TableName=conf['aws-deploy']['done-table-name'])
        except ClientError as err:
            conf['aws-deployed']['done-table'] = None

def create_reporting_dynamodb_table() -> None:
    client = boto3.client('dynamodb')
    try:
        client.describe_table(TableName=bert_reporting.TABLE_NAME)
    except ClientError as err:
        logger.info(f'Creating Dynamodb Table[{bert_reporting.TABLE_NAME}]')
        client.create_table(
            TableName=bert_reporting.TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'identity',
                    'KeyType': 'HASH',
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'identity',
                    'AttributeType': 'S',
                }
            ],
            StreamSpecification={
                'StreamEnabled': False,
            },
            BillingMode='PAY_PER_REQUEST')


def create_dynamodb_tables(jobs: typing.Dict[str, typing.Dict[str, typing.Any]]) -> None:
    client = boto3.client('dynamodb')
    for job_name, conf in jobs.items():
        try:
            conf['aws-deployed']['work-table'] = client.describe_table(TableName=conf['aws-deploy']['work-table-name'])
        except ClientError as err:
            logger.info(f'Creating Dynamodb Table[{conf["aws-deploy"]["work-table-name"]}]')
            client.create_table(
                    TableName=conf['aws-deploy']['work-table-name'],
                    KeySchema=[
                        {
                            'AttributeName': 'identity',
                            'KeyType': 'HASH'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'identity',
                            'AttributeType': 'S'
                        }
                    ],
                    StreamSpecification={
                        'StreamEnabled': True,
                        'StreamViewType': 'NEW_IMAGE'
                    },
                    BillingMode='PAY_PER_REQUEST')

            conf['aws-deployed']['work-table'] = client.describe_table(TableName=conf['aws-deploy']['work-table-name'])

        try:
            conf['aws-deployed']['done-table'] = client.describe_table(TableName=conf['aws-deploy']['done-table-name'])
        except ClientError as err:
            logger.info(f'Creating Dynamodb Table[{conf["aws-deploy"]["done-table-name"]}]')
            client.create_table(
                    TableName=conf['aws-deploy']['done-table-name'],
                    KeySchema=[
                        {
                            'AttributeName': 'identity',
                            'KeyType': 'HASH'
                        }
                    ],
                    AttributeDefinitions=[
                        {
                            'AttributeName': 'identity',
                            'AttributeType': 'S'
                        }
                    ],
                    StreamSpecification={
                        'StreamEnabled': True,
                        'StreamViewType': 'NEW_IMAGE'
                    },
                    BillingMode='PAY_PER_REQUEST')

            conf['aws-deployed']['done-table'] = client.describe_table(TableName=conf['aws-deploy']['done-table-name'])

def create_roles(jobs: typing.Dict[str, typing.Any]) -> None:
    trust_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {
                    "Service": [
                        # "apigateway.amazonaws.com",
                        "lambda.amazonaws.com", # Used to call lambdas from within another lambda
                        "events.amazonaws.com", # Used for Scheduling lambda execution
                        # "dynamodb.amazonaws.com",
                    ]
                },
                "Action": "sts:AssumeRole",
            }
        ]
    }
    region_name: str = boto3.session.Session().region_name
    account_id: str = boto3.client('sts').get_caller_identity().get('Account')
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:Get*",
                    "dynamodb:PutItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:Scan",
                    "dynamodb:DescribeStream",
                    "dynamodb:ListStreams"
                ],
                "Resource": f"arn:aws:dynamodb:{region_name}:{account_id}:table/*",
            },
            {
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:CreateLogGroup",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
            # These permissions need to be ironed out
            {
                "Sid": "CloudWatchEventsFullAccess",
                "Effect": "Allow",
                "Action": "events:*",
                "Resource": "*"
            },
            {
                "Sid": "IAMPassRoleForCloudWatchEvents",
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": "arn:aws:iam::*:role/AWS_Events_Invoke_Targets"
            }
        ]
    }
    trust_policy_name: str = 'bert-etl-lambda-execution-policy-trust'
    policy_name: str = 'bert-etl-lambda-execution-policy'
    iam_role = {
        'Path': '/',
        'RoleName': trust_policy_name,
        'AssumeRolePolicyDocument': json.dumps(trust_policy_document),
        'Description': 'Bert-ETL Lambda Execution Role',
    }
    iam_policy = {
        'Path': '/',
        'PolicyName': policy_name,
        'PolicyDocument': json.dumps(policy_document),
        'Description': 'Bert-ETL Lambda Execution Policy'
    }
    iam_client = boto3.client('iam')
    role = bert_deploy_shortcuts.map_iam_role(trust_policy_name)
    if role is None:
        iam_client.create_role(**iam_role)
        role = bert_deploy_shortcuts.map_iam_role(trust_policy_name)

    policy = bert_deploy_shortcuts.map_iam_policy(policy_name)
    if policy is None:
        iam_client.create_policy(**iam_policy)
        policy = bert_deploy_shortcuts.map_iam_policy(policy_name)
        iam_client.attach_role_policy(RoleName=trust_policy_name, PolicyArn=policy['Arn'])

    for job_name, conf in jobs.items():
        conf['aws-deployed']['iam-role'] = role
        conf['aws-deployed']['iam-policy'] = policy

def destroy_lambda_to_table_bindings(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('lambda')
    dynamodb_client = boto3.client('dynamodb')
    for job_name, conf in jobs.items():
        if conf['spaces']['parent']['noop-space'] is True:
            continue

        elif conf['spaces']['pipeline-type'] == bert_constants.PipelineType.BOTTLE:
            continue

        try:
            parent_table = dynamodb_client.describe_table(TableName=conf['spaces']['parent']['done-key'])
        except ClientError:
            return None

        parent_table_name = parent_table['Table']['TableName']
        parent_stream_arn = parent_table['Table']['LatestStreamArn']
        logger.info(f'Destroying mappings between Lambda[{job_name}] and Work-Table[{parent_table_name}]')
        for event_mapping in client.list_event_source_mappings(
                EventSourceArn=parent_stream_arn,
                FunctionName=conf['aws-deploy']['lambda-name'])['EventSourceMappings']:
            client.delete_event_source_mapping(UUID=event_mapping['UUID'])

def destroy_lambda_concurrency(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('lambda')
    for job_name, conf in jobs.items():
        try:
            client.delete_function_concurrency(FunctionName=conf['aws-deploy']['lambda-name'])
        except client.exceptions.ResourceNotFoundException:
            pass

def destroy_sns_topic_lambdas(jobs: typing.Dict[str, typing.Any]) -> None:
    lambda_client = boto3.client('lambda')
    sns_client = boto3.client('sns')
    for job_name, conf in jobs.items():
        if conf['spaces']['parent']['noop-space'] is True:
            try:
                conf['aws-deployed']['aws-lambda'] = lambda_client.get_function(FunctionName=conf['aws-deploy']['lambda-name'])['Configuration']
            except lambda_client.exceptions.ResourceNotFoundException:
                continue

            else:
                for page in sns_client.get_paginator('list_subscriptions').paginate():
                    for subscription in page['Subscriptions']:
                        if subscription['Endpoint']  == conf['aws-deployed']['aws-lambda']['FunctionArn']:
                            sns_client.unsubscribe(SubscriptionArn=subscription['SubscriptionArn'])


def destroy_lambdas(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('lambda')
    for job_name, conf in jobs.items():
        try:
            client.delete_function(FunctionName=conf['aws-deploy']['lambda-name'])

        except ClientError as err:
            pass

        else:
            logger.info(f'Deleted Lambda[{job_name}]')

def create_lambda_s3_item(job_name: str, conf: typing.Dict[str, typing.Any]) -> None:
    if conf['deployment']['s3_bucket'] is None:
        raise bert_exceptions.BertConfigError('Archive over 50 MB. Please specify an s3_bucket to upload to. https://bert-etl.readthedocs.io/en/latest/bert-etl.yaml#s3_bucket')

    client = boto3.client('s3')

    try:
        client.head_bucket(Bucket=conf['deployment']['s3_bucket'])
    except ClientError:
        logger.info(f'Creating Bucket[{conf["deployment"]["s3_bucket"]}]')
        try:
            client.create_bucket(Bucket=conf['deployment']['s3_bucket'])
        except ClientError:
            raise NotImplementedError('''
Should not contain uppercase characters
- Should not contain underscores (_)
- Should be between 3 and 63 characters long
- Should not end with a dash
- Cannot contain two, adjacent periods
- Cannot contain dashes next to periods (e.g., "my-.bucket.com" and "my.-bucket" are invalid)
''')

    filename: str = os.path.basename(conf['aws-build']['archive-path'])
    logger.info(f'Uploading file[{filename}] to bucket[{conf["deployment"]["s3_bucket"]}]')
    client.upload_file(conf['aws-build']['archive-path'], conf['deployment']['s3_bucket'], filename)
    return {
        'S3Bucket': conf['deployment']['s3_bucket'],
        'S3Key': filename,
    }

def create_lambdas(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('lambda')
    for job_name, conf in jobs.items():
        if conf['aws-build']['archive-size'] - 50000000 > 0:
            code_config = create_lambda_s3_item(job_name, conf)

        else:
            code_config = {
                'ZipFile': open(conf['aws-build']['archive-path'], 'rb').read()
            }


        try:
            client.get_function(FunctionName=conf['aws-deploy']['lambda-name'])['Configuration']
        except (ClientError, client.exceptions.ResourceNotFoundException) as err:
            logger.info(f'Creating AWSLambda for Job[{job_name}]')
            lambda_description = client.create_function(
                FunctionName=conf['aws-deploy']['lambda-name'],
                Runtime=conf['aws-deploy']['runtime'],
                MemorySize=conf['aws-deploy']['memory-size'],
                Role=conf['aws-deployed']['iam-role']['Arn'],
                Handler=conf['aws-deploy']['handler'],
                Code=code_config,
                Timeout=conf['aws-deploy']['timeout'],
                Environment={'Variables': conf['aws-deploy']['environment']})
            conf['aws-deployed']['aws-lambda'] = client.get_function(FunctionName=conf['aws-deploy']['lambda-name'])['Configuration']

        else:
            logger.info(f'Replacing AWSLambda for Job[{job_name}]')
            client.delete_function(FunctionName=conf['aws-deploy']['lambda-name'])
            client.create_function(
                FunctionName=conf['aws-deploy']['lambda-name'],
                Runtime=conf['aws-deploy']['runtime'],
                MemorySize=conf['aws-deploy']['memory-size'],
                Role=conf['aws-deploy']['iam-role']['Arn'],
                Handler=conf['aws-deploy']['handler'],
                Code=code_config,
                Timeout=conf['aws-deploy']['timeout'],
                Environment={'Variables': conf['aws-deploy']['environment']},
            )
            conf['aws-deployed']['aws-lambda'] = client.get_function(FunctionName=conf['aws-deploy']['lambda-name'])['Configuration']

def create_lambda_concurrency(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('lambda')
    for job_name, conf in jobs.items():
        if conf['aws-deploy']['concurrency-limit'] > 0:
            client.put_function_concurrency(
                FunctionName=conf['aws-deploy']['lambda-name'],
                ReservedConcurrentExecutions=conf['aws-deploy']['concurrency-limit'])

def bind_lambdas_to_tables(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('lambda')
    dynamodb_client = boto3.client('dynamodb')
    for job_name, conf in jobs.items():
        if conf['spaces']['parent']['noop-space'] is True:
            continue

        elif conf['spaces']['pipeline-type'] == bert_constants.PipelineType.BOTTLE:
            continue

        parent_table = dynamodb_client.describe_table(TableName=conf['spaces']['parent']['done-key'])
        parent_table_name = parent_table['Table']['TableName']
        parent_stream_arn = parent_table['Table']['LatestStreamArn']
        logger.info(f'Mapping Lambda[{job_name}] to Work-Table[{parent_table_name}]')
        client.create_event_source_mapping(
            EventSourceArn=parent_stream_arn,
            FunctionName=conf['aws-deploy']['lambda-name'],
            BatchSize=conf['aws-deploy']['batch-size'],
            MaximumBatchingWindowInSeconds=conf['aws-deploy']['batch-size-delay'],
            Enabled=True,
            StartingPosition='LATEST')


def bind_events_for_bottle_functions(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('events')
    for job_name, conf in jobs.items():
        if conf['spaces']['parent']['noop-space'] is False and bert_constants.PipelineType.BOTTLE == conf['spaces']['pipeline-type']:
            logger.info(f'Scheduling[{conf["bottle"]["schedule-expression"]}] Lambda[{job_name}] to Cloudwatch Events')

            rule_name: str = f'{job_name}-bottle-interval'
            target_id: str = f'{job_name}-bottle-target-id'
            conf['aws-deployed']['bottle']['schedule-expression-rule-name'] = rule_name
            conf['aws-deployed']['bottle']['schedule-expression-target-id'] = target_id
            # Make Rule
            try:
                conf['aws-deployed']['bottle']['schedule-expression-rule-rule'] = client.describe_rule(Name=conf['aws-deployed']['bottle']['schedule-expression-rule-name'])

            except client.exceptions.ResourceNotFoundException:
                client.put_rule(
                    Name=conf['aws-deployed']['bottle']['schedule-expression-rule-name'],
                    ScheduleExpression=conf['bottle']['schedule-expression'],
                    RoleArn=conf['aws-deployed']['iam-role']['Arn'],
                    State='ENABLED')

                conf['aws-deployed']['bottle']['schedule-expression-rule'] = client.describe_rule(Name=conf['aws-deployed']['bottle']['schedule-expression-rule-name'])

            else:
                for page in client.get_paginator('list_targets_by_rule').paginate(Rule=conf['aws-deployed']['bottle']['schedule-expression-rule-name']):
                    target_ids = [t['Id'] for t in page['Targets']]
                    if target_ids:
                        client.remove_targets(Rule=conf['aws-deployed']['bottle']['schedule-expression-rule-name'], Ids=target_ids)

                client.delete_rule(Name=conf['aws-deployed']['bottle']['schedule-expression-rule-name'])
                client.put_rule(
                    Name=conf['aws-deployed']['bottle']['schedule-expression-rule-name'],
                    ScheduleExpression=conf['bottle']['schedule-expression'],
                    RoleArn=conf['aws-deployed']['iam-role']['Arn'],
                    State='ENABLED')

                conf['aws-deployed']['bottle']['schedule-expression-rule'] = client.describe_rule(Name=conf['aws-deployed']['bottle']['schedule-expression-rule-name'])

            # Add permissions
            lambda_client = boto3.client('lambda')
            lambda_client.add_permission(
                FunctionName=conf['aws-deploy']['lambda-name'],
                StatementId=f'{conf["aws-deployed"]["bottle"]["schedule-expression-rule-name"]}-Event',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=conf['aws-deployed']['bottle']['schedule-expression-rule']['Arn'])

            # Remove old targets
            target = None
            for page in client.get_paginator('list_targets_by_rule').paginate(Rule=conf['aws-deployed']['bottle']['schedule-expression-rule-name']):
                target_ids = [target['Id'] for target in page['Targets']]
                if len(target_ids) > 0:
                    client.remove_targets(
                        Rule=conf['aws-deployed']['bottle']['schedule-expression-rule-name'],
                        Ids=target_ids,
                        Force=True)

            if target is None:
                client.put_targets(
                    Rule=conf['aws-deployed']['bottle']['schedule-expression-rule-name'],
                    Targets=[{
                        'Arn': conf['aws-deployed']['aws-lambda']['FunctionArn'],
                        'Id': conf['aws-deployed']['bottle']['schedule-expression-target-id'],
                    }])


def bind_events_for_init_function(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('events')
    for job_name, conf in jobs.items():
        if conf['spaces']['parent']['noop-space'] is True and conf['events']['schedule-expression']:
            logger.info(f'Scheduling[{conf["events"]["schedule-expression"]}] Lambda[{job_name}] to Cloudwatch Events')
            rule_name: str = f'{job_name}-interval'
            target_id: str = f'{job_name}-target-id'
            conf['aws-deployed']['events']['schedule-expression-rule-name'] = rule_name
            conf['aws-deployed']['events']['schedule-expression-target-id'] = target_id
            # Make Rule
            try:
                conf['aws-deployed']['events']['schedule-expression-rule-rule'] = client.describe_rule(Name=conf['aws-deployed']['events']['schedule-expression-rule-name'])

            except client.exceptions.ResourceNotFoundException:
                client.put_rule(
                    Name=conf['aws-deployed']['events']['schedule-expression-rule-name'],
                    ScheduleExpression=conf['events']['schedule-expression'],
                    RoleArn=conf['aws-deployed']['iam-role']['Arn'],
                    State='ENABLED')

                conf['aws-deployed']['events']['schedule-expression-rule'] = client.describe_rule(Name=conf['aws-deployed']['events']['schedule-expression-rule-name'])

            else:
                for page in client.get_paginator('list_targets_by_rule').paginate(Rule=conf['aws-deployed']['events']['schedule-expression-rule-name']):
                    target_ids = [t['Id'] for t in page['Targets']]
                    if target_ids:
                        client.remove_targets(Rule=conf['aws-deployed']['events']['schedule-expression-rule-name'], Ids=target_ids)

                client.delete_rule(Name=conf['aws-deployed']['events']['schedule-expression-rule-name'])
                client.put_rule(
                    Name=conf['aws-deployed']['events']['schedule-expression-rule-name'],
                    ScheduleExpression=conf['events']['schedule-expression'],
                    RoleArn=conf['aws-deployed']['iam-role']['Arn'],
                    State='ENABLED')

                conf['aws-deployed']['events']['schedule-expression-rule'] = client.describe_rule(Name=conf['aws-deployed']['events']['schedule-expression-rule-name'])

            # Add permissions
            lambda_client = boto3.client('lambda')
            lambda_client.add_permission(
                FunctionName=conf['aws-deploy']['lambda-name'],
                StatementId=f'{conf["aws-deployed"]["events"]["schedule-expression-rule-name"]}-Event',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com',
                SourceArn=conf['aws-deployed']['events']['schedule-expression-rule']['Arn'])

            # Remove old targets
            target = None
            for page in client.get_paginator('list_targets_by_rule').paginate(Rule=conf['aws-deployed']['events']['schedule-expression-rule-name']):
                target_ids = [target['Id'] for target in page['Targets']]
                if len(target_ids) > 0:
                    client.remove_targets(
                        Rule=conf['aws-deployed']['events']['schedule-expression-rule-name'],
                        Ids=target_ids,
                        Force=True)

            if target is None:
                client.put_targets(
                    Rule=conf['aws-deployed']['events']['schedule-expression-rule-name'],
                    Targets=[{
                        'Arn': conf['aws-deployed']['aws-lambda']['FunctionArn'],
                        'Id': conf['aws-deployed']['events']['schedule-expression-target-id'],
                    }])

        if conf['spaces']['parent']['noop-space'] is True and conf['events']['sns-topic-arn']:
            logger.info(f'Attaching Job[{job_name}] to SNS Topic[{conf["events"]["sns-topic-arn"]}]')
            sns_client = boto3.client('sns')
            events_client = boto3.client('events')
            lambda_client = boto3.client('lambda')

            for page in sns_client.get_paginator('list_subscriptions_by_topic').paginate(TopicArn=conf['events']['sns-topic-arn']):
                for subscription in page['Subscriptions']:
                    import ipdb; ipdb.set_trace()
                    pass

            conf['aws-deployed']['events']['sns-topic'] = sns_client.subscribe(
                TopicArn=conf['events']['sns-topic-arn'],
                Protocol='lambda',
                Endpoint=conf['aws-deployed']['aws-lambda']['FunctionArn'])

            lambda_client.add_permission(
                FunctionName=conf['aws-deploy']['lambda-name'],
                StatementId=f'{job_name}-sns-event',
                Action='lambda:InvokeFunction',
                Principal='sns.amazonaws.com',
                SourceArn=conf['events']['sns-topic-arn'])

            conf['aws-deployed']['events']['sns-rule-name'] = f'{job_name}-sns-rule'
            conf['aws-deployed']['events']['sns-target-id'] = f'{job_name}-sns-target'
            conf['aws-deployed']['events']['sns-event-pattern'] = json.dumps({
                'EventSource': ['aws.bert-etl-sns-topic'],
                'EventSourceArn': [conf['events']['sns-topic-arn']],
            })

            try:
                conf['aws-deployed']['events']['sns-rule'] = events_client.describe_rule(Name=conf['aws-deployed']['events']['sns-rule-name'])
            except events_client.exceptions.ResourceNotFoundException:
                events_client.put_rule(
                    Name=conf['aws-deployed']['events']['sns-rule-name'],
                    EventPattern=conf['aws-deployed']['events']['sns-event-pattern'],
                    RoleArn=conf['aws-deployed']['iam-role']['Arn'],
                    State='ENABLED')

            else:
                for page in events_client.get_paginator('list_targets_by_rule').paginate(Rule=conf['aws-deployed']['events']['sns-rule-name']):
                    target_ids = [target['Id'] for target in page['Targets']]
                    if len(target_ids) > 0:
                        events_client.remove_targets(
                            Rule=conf['aws-deployed']['events']['sns-rule-name'],
                            Ids=target_ids,
                            Force=True)

                events_client.delete_rule(Name=conf['aws-deployed']['events']['sns-rule-name'])
                events_client.put_rule(
                    Name=conf['aws-deployed']['events']['sns-rule-name'],
                    EventPattern=conf['aws-deployed']['events']['sns-event-pattern'],
                    RoleArn=conf['aws-deployed']['iam-role']['Arn'],
                    State='ENABLED')

            for page in events_client.get_paginator('list_targets_by_rule').paginate(Rule=conf['aws-deployed']['events']['sns-rule-name']):
                target_ids = [target['Id'] for target in page['Targets']]
                if len(target_ids) > 0:
                    events_client.remove_targets(
                        Rule=conf['aws-deployed']['events']['sns-rule-name'],
                        Ids=target_ids,
                        Force=True)

            events_client.put_targets(
                Rule=conf['aws-deployed']['events']['sns-rule-name'],
                Targets=[{
                    # 'Arn': conf['aws-deployed']['aws-lambda']['FunctionArn'],
                    'Arn': conf['events']['sns-topic-arn'],
                    'Id': conf['aws-deployed']['events']['sns-target-id'],
                }])

        # if conf['spaces']['parent']['noop-space'] is True and conf['events']['s3']:

