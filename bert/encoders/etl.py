import json
import types
import typing

from bert import constants

from datetime import datetime

from bert.etl import ETLReference
from bert.encoders import base
from bert.encoders.datatypes import BertETLEncodingMap

PWN = typing.TypeVar('PWN')

class ETLIdentityEncoder(json.JSONEncoder):
    def default(self: PWN, obj: typing.Any) -> typing.Any:
        if isinstance(obj, ETLReference):
            import pdb; pdb.set_trace()
            import sys; sys.exit(1)
            return obj.strftime(constants.DATETIME_FORMAT)

        elif hasattr(datum, '_payload') and datum.__class__.__name__ == 'QueueItem':
            return super(IdentityEncoder, self).default(datum._payload)

        import pdb; pdb.set_trace()
        return super(ETLIdentityEncoder, self).default(obj)

def _find_aws_encoding(datum: typing.Any) -> typing.Dict[str, typing.Any]:
    if isinstance(datum, ETLReference):
        return 'M'

    encoding_map = BertETLEncodingMap()
    encoding_map.add_map(ETLReference, _find_aws_encoding, encode_aws_object, decode_aws_object)
    return base.encode_aws_object(datum, encoding_map)

def _encode_etl_reference(etl_reference: ETLReference) -> typing.Dict[str, str]:
    return etl_reference.__class__.Serialize(etl_reference)

def _decode_etl_reference(etl_reference: ETLReference) -> typing.Dict[str, str]:
    import pdb; pdb.set_trace()
    raise NotImplementedError

def encode_aws_object(datum: typing.Any) -> typing.Dict[str, typing.Any]:
    if isinstance(datum, ETLReference):
        return _encode_etl_reference(datum)

    encoding_map = BertETLEncodingMap()
    encoding_map.add_map(ETLReference, _find_aws_encoding, encode_aws_object, decode_aws_object)
    return base.encode_aws_object(datum, encoding_map)

def decode_aws_object(datum: typing.Dict[str, typing.Any]) -> typing.Any:
    try:
        datum.items()
    except AttributeError:
        import pdb; pdb.set_trace()
        raise NotImplementedError

    if ETLReference.REF_KEY in datum.keys():
        return datum

    encoding_map = BertETLEncodingMap()
    encoding_map.add_map(ETLReference, _find_aws_encoding, encode_aws_object, decode_aws_object)
    return base.decode_aws_object(datum, encoding_map)

