#!/usr/env/bin python

import argparse
import hashlib
import logging
import os
import signal
import typing

from bert import \
    utils as bert_utils, \
    datasource as bert_datasource, \
    constants as bert_constants

logger = logging.getLogger(__name__)

STOP_DAEMON: bool = False
LOG_ERROR_ONLY: bool = False if os.environ.get('LOG_ERROR_ONLY', 'true') in ['f', 'false', 'no'] else True
MAX_RESTART: int = int(os.environ.get('MAX_RESTART_COUNT', 10))


def capture_options() -> typing.Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--module-name', required=True, help='https://bert-etl.readthedocs.io/en/latest/module_name.html')
    parser.add_argument('-p', '--print-job-spaces', default=False, action='store_true')
    parser.add_argument('-d', '--count-job-duplicates', default=False, action='store_true')
    parser.add_argument('-c', '--count-job-spaces', default=False, action='store_true')
    return parser.parse_args()

def print_job_spaces(options: argparse.Namespace):
    jobs = bert_utils.scan_jobs(options.module_name)
    jobs = bert_utils.map_jobs(jobs, options.module_name)
    for job_name, conf in jobs.items():
        job = conf['job']
        work_queue, done_queue, ologger = bert_utils.comm_binders(job)
        logging.info(f'Work Table Space[{work_queue._table_name}] for Job[{job_name}]')

    else:
        logging.info(f'Done Table Space[{done_queue._table_name}] for Job[{job_name}]')
       
def count_job_duplicates(options: argparse.Namespace) -> None:
    jobs = bert_utils.scan_jobs(options.module_name)
    jobs = bert_utils.map_jobs(jobs, options.module_name)
    client = bert_datasource.RedisConnection.ParseURL(bert_constants.REDIS_URL).client
    for job_name, conf in jobs.items():
        job = conf['job']
        work_queue, done_queue, ologger = bert_utils.comm_binders(job)
        step = 200
        offset = 0
        # No adjustment
        total = client.llen(work_queue._table_name)
        entry_hashes = []
        while len(entry_hashes) < total:
            next_items = client.lrange(work_queue._table_name, offset, offset + total)
            entry_hashes.extend([hashlib.sha256(entry).hexdigest()
                for entry in client.lrange(work_queue._table_name, offset, offset + total)])

        unique_hash_count = len([entry for entry in set(entry_hashes)])
        logger.info(f'Duplicate Entries for Job[{job_name}]: {len(entry_hashes) - unique_hash_count}; total: {len(entry_hashes)}')

def count_job_spaces(options: argparse.Namespace):
    jobs = bert_utils.scan_jobs(options.module_name)
    jobs = bert_utils.map_jobs(jobs, options.module_name)
    client = bert_datasource.RedisConnection.ParseURL(bert_constants.REDIS_URL).client
    for idx, (job_name, conf) in enumerate(jobs.items()):
        job = conf['job']
        work_queue, done_queue, ologger = bert_utils.comm_binders(job)
        if idx != len(jobs.keys()) - 1:
            total = client.llen(work_queue._table_name)
            logger.info(f'{job_name} Count[{total}] (job)')
            if job.cache_backend:
                cache_key = job.cache_backend._cache_key(job.cache_backend._done_tablename)
                total = client.llen(cache_key)
                logger.info(f'{job_name} Count[{total}] (cache)')

        else:
            total = client.llen(work_queue._table_name)
            logger.info(f'{job_name} Work Count[{total}]')

            total = client.llen(done_queue._table_name)
            logger.info(f'{options.module_name} Pipeline Count[{total}]')

def run_from_cli():
    import sys, os
    sys.path.append(os.getcwd())
    options = capture_options()
    if options.print_job_spaces:
        print_job_spaces(options)
        sys.exit(0)

    elif options.count_job_duplicates:
        count_job_duplicates(options)
        sys.exit(0)

    elif options.count_job_spaces:
        count_job_spaces(options)

if __name__ in ['__main__']:
    run_from_cli()
