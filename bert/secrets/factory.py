#!/usr/env/bin python

import argparse
import logging
import os
import signal
import types
import typing

from bert import \
    utils as bert_utils, \
    shortcuts as bert_shortcuts, \
    exceptions as bert_exceptions, \
    aws as bert_aws

logger = logging.getLogger(__name__)

STOP_DAEMON: bool = False
LOG_ERROR_ONLY: bool = False if os.environ.get('LOG_ERROR_ONLY', 'true') in ['f', 'false', 'no'] else True
MAX_RESTART: int = int(os.environ.get('MAX_RESTART_COUNT', 10))


def capture_options() -> typing.Any:
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--keypath', default=None, required=False)
    parser.add_argument('-e', '--encrypt', default=False, required=False, action='store_true')
    parser.add_argument('-d', '--decrypt', default=False, required=False, action='store_true')
    parser.add_argument('-o', '--dry-run-off', default=False, action='store_true')
    parser.add_argument('-u', '--update-key-policy', default=False, action='store_true')
    return parser.parse_args()


def run_secrets(options: argparse.Namespace) -> None:
    bert_configuration = bert_shortcuts.load_configuration() or {}
    secrets = bert_shortcuts.obtain_secrets_config(bert_configuration)

    with bert_aws.kms(secrets.key_alias, secrets.usernames, True) as keymaster:
        if options.encrypt:
            value: typing.Any = bert_shortcuts.load_if_exists(options.keypath, bert_configuration)
            logger.info(f'Encrypting Keypath[{options.keypath}]')
            if isinstance(value, dict):
                for key, sub_value in value.items():
                    value[key] = keymaster.encrypt(sub_value)

            elif isinstance(value, list):
                for idx, sub_value in enumerate(value):
                    value[idx] = keymaster.encrypt(sub_value)

            elif isinstance(value, str):
                value = keymaster.encrypt(value)

            else:
                raise NotImplementedError

            bert_shortcuts.write_keypath_value(options.keypath, bert_configuration, value)

        elif options.decrypt:
            value: typing.Any = bert_shortcuts.load_if_exists(options.keypath, bert_configuration)
            logger.info(f'Decrypting Keypath[{options.keypath}]')
            if isinstance(value, dict):
                for key, sub_value in value.items():
                    value[key] = keymaster.decrypt(sub_value)

            elif isinstance(value, list):
                for idx, sub_value in enumerate(value):
                    value[idx] = keymaster.decrypt(sub_value)

            elif isinstance(value, str):
                value = keymaster.decrypt(value)

            else:
                raise NotImplementedError

            bert_shortcuts.write_keypath_value(options.keypath, bert_configuration, value)

        elif options.update_key_policy:
            keymaster.update_usernames()

        else:
            raise NotImplementedError

    if options.dry_run_off is True:
        bert_shortcuts.save_configuration(bert_configuration)

def run_from_cli():
    import sys, os
    sys.path.append(os.getcwd())
    options = capture_options()
    run_secrets(options)

if __name__ in ['__main__']:
    run_from_cli()

