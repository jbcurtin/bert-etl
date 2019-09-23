import base64
import json
import types
import typing

import numpy as np

from bert.encoders import base as base_encoders

PWN = typing.TypeVar('PWN')

class NumpyIdentityEncoder(base_encoders.IdentityEncoder):
    def default(self: PWN, obj: typing.Any) -> typing.Any:
        if isinstance(obj, (
            np.float32, np.float64, np.float16,
            np.complex64,
            np.int8, np.int16, np.int32, np.int64,
            np.uint8, np.uint16, np.uint32, np.uint64,
            np.intc, np.intp,
            np.bool_,
            np.ndarray)):
            return base64.b64encode(obj.tostring(order='F')).decode('ascii')

        return super(NumpyIdentityEncoder, self).default(obj)

def _find_aws_encoding(datum: typing.Any) -> typing.Dict[str, typing.Any]:
    if isinstance(datum, (
        np.float32, np.float64, np.float16,
        np.complex64,
        np.int8, np.int16, np.int32, np.int64,
        np.uint8, np.uint16, np.uint32, np.uint64,
        np.intc, np.intp,
        np.bool_,
        np.ndarray)):
        return 'S'

    return base_encoders._find_aws_encoding(datum)

def encode_aws_object(datum: typing.Any) -> typing.Dict[str, typing.Any]:
    if isinstance(datum, dict):
        for key, value in datum.items():
            datum[key] = {_find_aws_encoding(value): encode_aws_object(value)}

        return datum

    elif isinstance(datum, (list, tuple, types.GeneratorType)):
        for idx, value in enumerate(datum):
            datum[idx] = {_find_aws_encoding(value): encode_aws_object(value)}

        return datum

    # Floats
    elif isinstance(datum, np.float32):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.float32:{encoded_value}'

    elif isinstance(datum, np.float64):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.float64:{encoded_value}'

    elif isinstance(datum, np.float16):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.float16:{encoded_value}'

    # Complex
    elif isinstance(datum, np.complex64):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.complex64:{encoded_value}'

    # Ints
    elif isinstance(datum, np.int8):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.int8:{encoded_value}'

    elif isinstance(datum, np.int16):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.int16:{encoded_value}'

    elif isinstance(datum, np.int32):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.int32:{encoded_value}'

    elif isinstance(datum, np.int64):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.int64:{encoded_value}'

    # Uint
    elif isinstance(datum, np.uint8):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.uint8:{encoded_value}'

    elif isinstance(datum, np.uint16):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.uint16:{encoded_value}'

    elif isinstance(datum, np.uint32):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.uint32:{encoded_value}'

    elif isinstance(datum, np.uint64):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.uint64:{encoded_value}'

    # intc, intp, bool
    elif isinstance(datum, np.intc):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.intc:{encoded_value}'

    elif isinstance(datum, np.intp):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.incp:{encoded_value}'

    elif isinstance(datum, np.bool_):
        encoded_value = base64.b64encode(datum.tostring()).decode('ascii')
        return f'np.bool_:{encoded_value}'

    elif isinstance(datum, np.ndarray):
        shape = str(datum.shape).strip('()')
        shape = ','.join(shape.split(', '))
        dtype = datum.dtype.type.__name__
        encoded_value = base64.b64encode(datum.tostring(order='F')).decode('ascii')
        return f'np.ndarray:{shape}:{dtype}:{encoded_value}'

    return base_encoders.encode_aws_object(datum)

def decode_aws_object(datum: typing.Dict[str, typing.Any]) -> typing.Any:
    try:
        datum.items()
    except Exception as err:
        import ipdb; ipdb.set_trace()
        pass
    for encoding_type, encoded in datum.items():
        if encoding_type == 'M':
            for key, value, in encoded.items():
                encoded[key] = decode_aws_object(value)

            return encoded

        elif encoding_type == 'L':
            for idx, value in enumerate(encoded):
                encoded[idx] = decode_aws_object(value)

            return encoded

        elif encoding_type == 'S' and encoded[:11] == 'np.float16:':
            binary = base64.b64decode(encoded[11:])
            return np.fromstring(binary, dtype=np.float16)[0]

        elif encoding_type == 'S' and encoded[:11] == 'np.float32:':
            binary = base64.b64decode(encoded[11:])
            return np.fromstring(binary, dtype=np.float32)[0]

        elif encoding_type == 'S' and encoded[:11] == 'np.float64:':
            binary = base64.b64decode(encoded[11:])
            return np.fromstring(binary, dtype=np.float64)[0]

        # np.complex64
        elif encoding_type == 'S' and encoded[:12] == 'np.complex64:':
            binary = base64.b64decode(encoded[12:])
            return np.fromstring(binary, dtype=np.complex64)[0]

        # np.int*
        elif encoding_type == 'S' and encoded[:8] == 'np.int8:':
            binary = base64.b64decode(encoded[8:])
            return np.fromstring(binary, dtype=np.int8)[0]

        elif encoding_type == 'S' and encoded[:9] == 'np.int16:':
            binary = base64.b64decode(encoded[9:])
            return np.fromstring(binary, dtype=np.int16)[0]

        elif encoding_type == 'S' and encoded[:9] == 'np.int32':
            binary = base64.b64decode(encoded[9:])
            return np.fromstring(binary, dtype=np.int32)[0]

        elif encoding_type == 'S' and encoded[:9] == 'np.int64':
            binary = base64.b64decode(encoded[9:])
            return np.fromstring(binary, dtype=np.int64)[0]

        # np.uint*
        elif encoding_type == 'S' and encoded[:9] == 'np.uint8:':
            binary = base64.b64decode(encoded[9:])
            return np.fromstring(binary, dtype=np.uint8)[0]

        elif encoding_type == 'S' and encoded[:10] == 'np.uint16:':
            binary = base64.b64decode(encoded[10:])
            return np.fromstring(binary, dtype=np.uint8)[0]

        elif encoding_type == 'S' and encoded[:10] == 'np.uint32:':
            binary = base64.b64decode(encoded[10:])
            return np.fromstring(binary, dtype=np.uint32)[0]

        elif encoding_type == 'S' and encoded[:10] == 'np.uint64:':
            binary = base64.b64decode(encoded[10:])
            return np.fromstring(binary, dtype=np.uint64)[0]

        # np.intc/p
        elif encoding_type == 'S' and encoded[:8] == 'np.intc:':
            binary = base64.b64decode(encoded[8:])
            return np.fromstring(binary, dtype=np.intc)[0]

        elif encoding_type == 'S' and encoded[:8] == 'np.intp:':
            binary = base64.b64decode(encoded[8:])
            return np.fromstring(binary, dtype=np.intp)[0]

        elif encoding_type == 'S' and encoded[:11] == 'np.ndarray:':
            shape, dtype, value = datum[11:].split(':', 2)
            dtype_value = {
                'int8': np.int8,
                'int16': np.int16,
                'int32': np.int32,
                'int64': np.int64,
                'float16': np.float16,
                'float32': np.float32,
                'float64': np.float64,
                'complex': np.complex,
                'complex64': np.complex64,
                'uint8': np.uint8,
                'uint16': np.uint16,
                'unit32': np.uint32,
                'uint64': np.uint64,
                'intc': np.intc,
                'intp': np.intp,
                'bool': np.bool_,
                'bool_': np.bool_
            }.get(dtype, None)
            if dtype_value is None:
                raise NotImplementedError(f'np.ndarray dtype[{dtype}] is not supported. Open an issue request for support: https://github.com/jbcurtin/bert-etl/issues')

            shape: typing.List[int] = [vec for vec in map(lambda x: int(x), shape.split(','))]
            value: bytes = base64.b64decode(value)
            # finding some way to tell this function we're using Fortran order would be nice to have
            return np.fromstring(value, dtype=dtype_value).reshape(*shape)

    return base_encoders.decode_aws_object(datum)

