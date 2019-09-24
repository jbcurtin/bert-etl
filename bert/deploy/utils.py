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

from bert import utils as bert_utils, encoders as bert_encoders, exceptions as bert_exceptions
from bert.deploy import shortcuts as bert_deploy_shortcuts, exceptions as deploy_exceptions
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
        job_templates: str = ''.join([f"""def {jn}():
    pass
""" for jn in jobs.keys() if jn != job_name])
        source_code: str = """
import typing
%s

from bert import utils, constants, binding, shortcuts, encoders

encoders.load_identity_encoders(%s)
encoders.load_queue_encoders(%s)
encoders.load_queue_decoders(%s)

%s

def %s_handler(event: typing.Dict[str, typing.Any] = {}, context: 'lambda_context' = None) -> None:

    records: typing.List[typing.Dict[str, typing.Any]] = event.get('Records', [])
    if len(records) > 0 and constants.DEBUG == False:
        constants.QueueType = constants.QueueTypes.StreamingQueue
        work_queue, done_queue, ologger = utils.comm_binders(%s)
        for record in records:
            if record['eventName'].lower() == 'INSERT'.lower():
                work_queue.local_put(record['dynamodb']['NewImage'])

    elif constants.DEBUG:
        constants.QueueType = constants.QueueTypes.LocalQueue
        work_queue, done_queue, ologger = utils.comm_binders(%s)
        work_queue.local_put(event)

    else:
        work_queue, done_queue, ologger = utils.comm_binders(%s)

    ologger.info(f'QueueType[{constants.QueueType}]')
    %s()

""" % (
    job_templates,
    conf['encoding']['identity_encoders'],
    conf['encoding']['queue_encoders'],
    conf['encoding']['queue_decoders'],
    '\n'.join(job_source),
    job_name,
    job_name,
    job_name,
    job_name,
    job_name)

        conf['lambda-path'] = os.path.join(conf['aws-build']['path'], f'{job_name}.py')
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
            logger.info(f'Mergeing Job[{job_name}] requirements')
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
                        "lambda.amazonaws.com",
                        # "events.amazonaws.com",
                        # "dynamodb.amazonaws.com",
                    ]
                },
                "Action": "sts:AssumeRole",
            }
        ]
    }
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:*"
                ],
                "Resource": "arn:aws:dynamodb:*:*:table/bert-etl-*",
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
    for job_name, conf in jobs.items():
        if conf['aws-deployed']['work-table'] is None:
            continue

        logger.info(f'Destroying mappings between Lambda[{job_name}] and Work-Table[{conf["aws-deploy"]["work-table-name"]}]')
        for event_mapping in client.list_event_source_mappings(
                EventSourceArn=conf['aws-deployed']['work-table']['Table']['LatestStreamArn'],
                FunctionName=conf['aws-deploy']['lambda-name'])['EventSourceMappings']:
            client.delete_event_source_mapping(UUID=event_mapping['UUID'])

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
        except ClientError as err:
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
            conf['aws-deployed']['aws-lambda'] = client.get_function(FunctionName=conf['aws-deploy']['lambda-name'])

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
            conf['aws-lambda'] = client.get_function(FunctionName=conf['aws-deploy']['lambda-name'])['Configuration']

def bind_lambdas_to_tables(jobs: typing.Dict[str, typing.Any]) -> None:
    client = boto3.client('lambda')
    for job_name, conf in jobs.items():
        logger.info(f'Mapping Lambda[{job_name}] to Work-Table[{conf["aws-deploy"]["work-table-name"]}]')
        client.create_event_source_mapping(
            EventSourceArn=conf['aws-deployed']['work-table']['Table']['LatestStreamArn'],
            FunctionName=conf['aws-deploy']['lambda-name'],
            Enabled=True,
            StartingPosition='LATEST')

