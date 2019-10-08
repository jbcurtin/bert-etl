import hashlib
import typing
import types

from bert import constants

def calc_func_space(func: typing.Union[str, types.FunctionType]) -> str:
    if isinstance(func, str):
        combined: str = hashlib.sha1(func.encode(constants.ENCODING)).hexdigest()
        return f'bert-etl-{combined}'

    elif isinstance(func, types.FunctionType):
        combined: str = hashlib.sha1(func.__name__.encode(constants.ENCODING)).hexdigest()
        return f'bert-etl-{combined}'

    else:
        raise NotImplementedError

def calc_func_key(*args: typing.List[str]) -> str:
    combined: str = hashlib.sha1(''.join(args).encode(constants.ENCODING)).hexdigest()
    return f'bert-etl-{combined}'

