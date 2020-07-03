import hashlib
import importlib
import types
import typing

PWN = typing.TypeVar('PWN')
ENCODING = 'utf-8'

from bert.etl import ETLReference

class BertETLEncodingMap:
    def __init__(self: PWN, encoding_map: typing.Dict['_class_type', '_function_type'] = None) -> None:
        self._map = {}
        if encoding_map:
            self.add_map(encoding_map)


    def add_map(self: PWN,
            _class_type: '_class_type',
            find_aws_encoding_func: types.FunctionType,
            encode_aws_object_func: types.FunctionType,
            decode_aws_object_func: types.FunctionType) -> None:

        if _class_type in self._map.keys():
            raise NotImplementedError(f'Unable to add _class_type[{_class_type}] to Map because it already exists')

        self._map[_class_type] = {
          'find_aws_encoding': find_aws_encoding_func,
          'encode_aws_object': encode_aws_object_func,
          'decode_aws_object': decode_aws_object_func,
        }

    def find_aws_encoding(self: PWN, datum: typing.Any) -> typing.Any:
        for _class_type, _func_map in self._map.items():
            if isinstance(datum, _class_type):
                return self.sign(_func_map['find_aws_encoding'](datum))

    def encode_aws_object(self: PWN, datum: typing.Any) -> typing.Any:
        for _class_type, _func_map in self._map.items():
            if isinstance(datum, _class_type):
                return self.sign(_func_map['encode_aws_object'](datum))

    def decode_aws_object(self: PWN, encoding_type: str, encoded: typing.Any) -> None:
        import pdb; pdb.set_trace()
        raise NotImplementedError

    REF_KEY = 'bert-etl-encoding-map-signature'
    def sign(self: PWN, datum: typing.Any) -> typing.Any:
        if not isinstance(datum, dict):
            return datum

        if self.REF_KEY in datum.keys():
            raise NotImplementedError

        keys = [k for k in sorted(datum.keys()) if k]
        key_mesh = ''.join(keys)
        key_hash = hashlib.sha256(key_mesh.encode(ENCODING)).hexdigest()
        datum[self.REF_KEY] = key_hash
        return datum

    def resolve_signature(self: PWN, datum: typing.Dict[str, typing.Any]) -> '_class_type':
        for key in [ETLReference.REF_KEY]:
            try:
                module_path, class_name = datum.get(key, None).rsplit('.', 1)
            except AttributeError:
                import pdb; pdb.set_trace()
                import sys; sys.exit(1)

            else:
                break

        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError:
            raise NotImplementedError

        _class_type = getattr(module, class_name, None)
        if _class_type is None:
            raise NotImplementedError

        return _class_type.Deserialize(datum).resolve()

