import boto3
import hashlib
import inspect
import os
import types
import typing

import functools as python_functools

from bert.etl import ETLState
from bert.etl.constants import BERT_ETL_S3_PREFIX
from bert.etl.sync_utils import upload_dataset, download_dataset

"""
cache_function_results
    The desigh behind cache_function_results comes from the want to support @decorator and @decorator() formatting.
      Allowing the user to be lazy in the API invocation design, and still have a consistent result. Current
      implementation only accounts for simple python datatypes. However the structure allows for complex encodings
      to happen. Allowing for more complex data-types to be cached and returned
"""

ENCODING = 'utf-8'
PWN = typing.TypeVar('PWN')
class cache_function_results:
    _setup: False
    def __init__(self: PWN, func_or_prefix: typing.Union[str, types.FunctionType]) -> None:
        self._setup = False
        if isinstance(func_or_prefix, str):
            self._prefix = f'{BERT_ETL_S3_PREFIX}/func-tools/{func_or_prefix}'

        elif isinstance(func_or_prefix, types.FunctionType):
            func_path = f'{func_or_prefix.__module__}.{func_or_prefix.__name__}'
            self._prefix = f'{BERT_ETL_S3_PREFIX}/func-tools/{func_path}'
            self._setup_func(func_or_prefix)
            # self._setup_cache(func_or_prefix)

        else:
            raise NotImplementedError

    def _hash_inputs(self: PWN, *args, **kwargs) -> str:
        properties = []
        for arg in args:
            if not isinstance(arg, str):
                properties.append(str(arg))

            else:
                properties.append(arg)

    def _setup_func(self: PWN, func: types.FunctionType) -> None:
        func_spec = inspect.getfullargspec(func)
        # Only supports data-types that can be converted into strs right now
        # func_args_spec = ''.join([str(value) for value in func_spec.args[:]])
        # func_annotation_spec = ''.join([':'.join([key, str(value)]) for key, value in func_spec.annotations.items()])
        # func_default_spec = ''.join([str(value) for value in func_spec.defaults[:]])
        # func_kword_spec = ''.join([str(value) for value in func_spec.kwonlyargs[:]])
        func_spec_key = ''.join([
            ''.join([str(value) for value in func_spec.args[:]]),
            ''.join([':'.join([key, str(value)]) for key, value in func_spec.annotations.items()]),
            ''.join([str(value) for value in func_spec.defaults[:]]),
            ''.join([str(value) for value in func_spec.kwonlyargs[:]]),
        ])
        etl_state_key = hashlib.sha256(func_spec_key.encode(ENCODING)).hexdigest()
        etl_state = ETLState(etl_state_key)

        # A question now rises in my mind. Do we want to pull down and maintain a global state from the __init__
        #  method? Or is it better to run this logic in the function invocation?
        @python_functools.wraps(func)
        def _wrapper(*args, **kwargs) -> typing.Any:
            func_invocation_key = ''.join([
                ''.join([str(arg) for arg in args]),
                ''.join([':'.join([key, value]) for key, value in kwargs.items()]),
            ])

            s3_key = ''.join([self._prefix, func_spec_key, func_invocation_key])
            s3_key = hashlib.sha256(s3_key.encode(ENCODING)).hexdigest()
            s3_key = f'{self._prefix}/{s3_key}'
            etl_state.localize()
            if etl_state.contains(s3_key) is True:
                func_result = download_dataset(s3_key, os.environ['DATASET_BUCKET'], dict)
                return func_result

            else:
                func_result = func(*args, **kwargs)
                # We'll need to expand the followirg logic so that caching can cache more than simple python datatypes
                upload_dataset(func_result, s3_key, os.environ['DATASET_BUCKET'])
                # Add to etl_state after the upload, incase an error occurs during upload
                etl_state.contain(s3_key)
                etl_state.synchronize()
                return func_result

        self._wrapped_func = _wrapper
        return _wrapper

    def __call__(self: PWN, func: types.FunctionType, *rest, **kwargs) -> typing.Any:
        if isinstance(func, types.FunctionType):
            return self._setup_func(func)

        first_arg = func,
        func_args = first_arg + rest
        return self._wrapped_func(*func_args, **kwargs)

