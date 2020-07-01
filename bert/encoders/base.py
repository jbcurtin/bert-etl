import json
import types
import typing

from bert import constants
from bert.encoders.datatypes import BertETLEncodingMap

from datetime import datetime

PWN = typing.TypeVar('PWN')

class IdentityEncoder(json.JSONEncoder):
    def default(self: PWN, obj: typing.Any) -> typing.Any:
        if isinstance(obj, datetime):
            return obj.strftime(constants.DATETIME_FORMAT)

        elif hasattr(datum, '_payload') and datum.__class__.__name__ == 'QueueItem':
            return super(IdentityEncoder, self).default(datum._payload)

        return super(IdentityEncoder, self).default(obj)

def _find_aws_encoding(
        datum: typing.Any,
        encoding_map: BertETLEncodingMap = None) -> typing.Dict[str, typing.Any]:
    if isinstance(datum, dict):
        return 'M'

    elif isinstance(datum, list):
        return 'L'

    elif isinstance(datum, bytes):
        return 'B'

    elif isinstance(datum, str):
        return 'S'

    elif isinstance(datum, bool):
        return 'S'

    elif isinstance(datum, int):
        return 'S'

    elif isinstance(datum, float):
        return 'S'

    elif hasattr(datum, '_payload') and datum.__class__.__name__ == 'QueueItem':
        return 'M'

    elif datum is None:
        return 'S'

    else:
        if encoding_map:
            return encoding_map.find_aws_encoding(datum)

    raise NotImplementedError
    
def encode_aws_object(
        datum: typing.Any,
        encoding_map: BertETLEncodingMap = None) -> typing.Dict[str, typing.Any]:

    if isinstance(datum, dict):
        for key, value in datum.items():
            datum[key] = {_find_aws_encoding(value, encoding_map): encode_aws_object(value, encoding_map)}

        return datum

    elif isinstance(datum, (list, tuple, types.GeneratorType)):
        for idx, value in enumerate(datum):
            datum[idx] = {_find_aws_encoding(value, encoding_map): encode_aws_object(value, encoding_map)}

        return datum

    elif isinstance(datum, bool):
        return f'bool:{datum}'

    elif isinstance(datum, int):
        return f'int:{datum}'

    elif isinstance(datum, float):
        return f'float:{datum}'

    elif isinstance(datum, str):
        return datum

    elif hasattr(datum, '_payload') and datum.__class__.__name__ == 'QueueItem':
        return encode_aws_object(datum._payload, encoding_map)

    elif datum is None:
        return 'null:'

    else:
        if encoding_map:
            return encoding_map.encode_aws_object(datum)

        raise NotImplementedError

    return None

def decode_aws_object(
        datum: typing.Dict[str, typing.Any],
        encoding_map: BertETLEncodingMap = None) -> typing.Any:

    for encoding_type, encoded in datum.items():
        if encoding_type == 'M':
            if BertETLEncodingMap.REF_KEY in encoded.keys():
                if encoding_map is None:
                    raise NotImplementedError(f'Encoding Map required to decode object')

                return encoding_map.resolve_signature(encoded)

            else:
                for key, value, in encoded.items():
                    encoded[key] = decode_aws_object(value, encoding_map)

                return encoded

        elif encoding_type == 'L':
            for idx, value in enumerate(encoded):
                encoded[idx] = decode_aws_object(value, encoding_map)

            return encoded

        elif encoding_type == 'B':
            return encoded

        elif encoding_type == 'S' and encoded[:5] == 'bool:':
            value: str = encoded.split(':', 1)[1].lower()
            if value == 'true':
                return True

            elif value == 'false':
                return False

            raise NotImplementedError(f'Unable to decode datum[{value}]')

        elif encoding_type == 'S' and encoded[:4] == 'int:':
            return int(encoded.split(':', 1)[1])

        elif encoding_type == 'S' and encoded[:6] == 'float:':
            return float(encoded.split(':', 1)[1])

        elif encoding_type == 'S' and encoded[:5] == 'null:':
            return None

        elif encoding_type == 'S':
            return encoded

        else:
            if encoding_map:
                return encoding_map.decode_aws_object(encoding_type, encoded)

        return None

