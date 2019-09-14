import json
import types
import typing

from bert import constants

from datetime import datetime

PWN = typing.TypeVar('PWN')

class IdentityEncoder(json.JSONEncoder):
    def default(self: PWN, obj: typing.Any) -> typing.Any:
        if isinstance(obj, datetime):
            return obj.strftime(constants.DATETIME_FORMAT)

        return super(IdentityEncoder, self).default(obj)

def _find_aws_encoding(datum: typing.Any) -> typing.Dict[str, typing.Any]:
    if isinstance(datum, dict):
        return 'M'

    elif isinstance(datum, list):
        return 'L'

    elif isinstance(datum, bytes):
        return 'B'

    elif isinstance(datum, str):
        return 'S'

    elif isinstance(datum, int):
        return 'S'

    elif isinstance(datum, float):
        return 'S'

    raise NotImplementedError(f'Unable to encode[{type(datum)}] datum[{datum}]')

def encode_aws_object(datum: typing.Any) -> typing.Dict[str, typing.Any]:
    if isinstance(datum, dict):
        for key, value in datum.items():
            datum[key] = {_find_aws_encoding(value): encode_aws_object(value)}

        return datum

    elif isinstance(datum, (list, tuple, types.GeneratorType)):
        for idx, value in enumerate(datum):
            datum[idx] = {_find_aws_encoding(value): encode_aws_object(value)}

        return datum

    elif isinstance(datum, int):
        return f'int:{datum}'

    elif isinstance(datum, float):
        return f'float:{datum}'

    elif isinstance(datum, str):
        return datum

    return None

def decode_aws_object(datum: typing.Dict[str, typing.Any]) -> typing.Any:
    for encoding_type, encoded in datum.items():
        if encoding_type == 'M':
            for key, value, in encoded.items():
                encoded[key] = decode_aws_object(value)

            return encoded

        elif encoding_type == 'L':
            for idx, value in enumerate(encoded):
                encoded[idx] = decode_aws_object(value)

            return encoded

        elif encoding_type == 'B':
            return encoded

        elif encoding_type == 'S' and encoded[:4] == 'int:':
            return int(encoded.split(':', 1)[1])

        elif encoding_type == 'S' and encoded[:6] == 'float:':
            return float(encoded.split(':', 1)[1])

        elif encoding_type == 'S':
            return encoded

        return None

