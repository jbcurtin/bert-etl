import importlib
import json
import types
import typing

from bert import exceptions as bert_exceptions

import logging
logger = logging.getLogger(__name__)

PWN = typing.TypeVar('PWN')

_ENCODER_OR_DECODER_CACHE: typing.Dict[str, typing.Any] = {}

# Job Execution API
IDENTITY_ENCODERS: typing.List[typing.Any] = []
QUEUE_ENCODERS: typing.List[typing.Any] = []
QUEUE_DECODERS: typing.List[typing.Any] = []

def load_encoders_or_decoders(encoders_or_decoders: typing.List[str]) -> None:
    objects: typing.List[typing.Any] = []
    for encoder_or_decoder in encoders_or_decoders:
        if encoder_or_decoder in _ENCODER_OR_DECODER_CACHE.keys():
            objects.append(_ENCODER_OR_DECODER_CACHE[encoder_or_decoder])
            continue

        try:
            module_path, encoder_or_decoder_name = encoder_or_decoder.rsplit('.', 1)
            module = importlib.import_module(module_path)

        except ValueError as err:
            raise bert_exceptions.EncoderLoadError(f'Unable to load Encoder/Decoder[{encoder_or_decoder}]. https://bert-etl.readthedocs.io/en/latest/encoders_and_decoders.html')

        except ImportError as err:
            raise bert_exceptions.EncoderLoadError(f'Unable to load Encoder/Decoder[{encoder_or_decoder}]. https://bert-etl.readthedocs.io/en/latest/encoders_and_decoders.html')

        else:
            encoder_or_decoder_object = getattr(module, encoder_or_decoder_name, None)
            if encoder_or_decoder_object is None:
                raise bert_exceptions.EncoderLoadError(f'Unable to load Encoder/Decoder[{encoder_or_decoder}]. https://bert-etl.readthedocs.io/en/latest/encoders_and_decoders.html')

            _ENCODER_OR_DECODER_CACHE[encoder_or_decoder] = encoder_or_decoder_object
            objects.append(encoder_or_decoder_object)

    return objects

def load_identity_encoders(identity_encoders: typing.List[str]) -> None:
    global IDENTITY_ENCODERS
    IDENTITY_ENCODERS = load_encoders_or_decoders(identity_encoders)

def load_queue_encoders(queue_encoders: typing.List[str]) -> None:
    global QUEUE_ENCODERS
    QUEUE_ENCODERS = load_encoders_or_decoders(queue_encoders)

def load_queue_decoders(queue_decoders: typing.List[str]) -> None:
    global QUEUE_DECODERS
    QUEUE_DECODERS = load_encoders_or_decoders(queue_decoders)

def clear_encoding() -> None:
    global QUEUE_ENCODERS
    global QUEUE_DECODERS
    global IDENTITY_ENCODERS
    QUEUE_ENCODERS = []
    QUEUE_DECODERS = []
    IDENTITY_ENCODERS = []

def encode_identity_object(obj: typing.Any) -> typing.Any:
    for identity_encoder in IDENTITY_ENCODERS:
        try:
            return json.dumps(obj, cls=identity_encoder)
        except TypeError:
            continue

    raise bert_exceptions.BertIdentityEncoderError(f'''
Unable to encoding identity with object[{obj}].
Datatype[{type(obj)}].
Loaded IdentityEncoders[{IDENTITY_ENCODERS}].
https://bert-etl.readthedocs.io/en/latest/encoders_and_decoders.html''')

def encode_object(obj: typing.Any) -> typing.Any:
    for encoder in QUEUE_ENCODERS:
        logger.debug(f'Encoder[{encoder.__module__}:{encoder.__name__}]')

    for encoder in QUEUE_ENCODERS:
        result = encoder(obj)
        logger.debug(f'Encoder[{encoder.__module__}:{encoder.__name__}] Result[{result}]')

        if result:
            return result

    raise bert_exceptions.BertEncoderError(f'''
Unable to encode object[{obj}].
Datatype[{obj}].
Loaded Encoders[{QUEUE_ENCODERS}].
https://bert-etl.readthedocs.io/en/latest/encoders_and_decoders.html''')

def decode_object(obj: typing.Any) -> typing.Any:
    for decoder in QUEUE_DECODERS:
        logger.debug(f'Decoder[{decoder.__module__}:{decoder.__name__}]')

    for decoder in QUEUE_DECODERS:
        result = decoder(obj)
        logger.debug(f'Decoder[{decoder.__module__}:{decoder.__name__}] Result[{result}]')

        if result:
            return result

    raise bert_exceptions.BertDecoderError(f'''
Unable to decode object[{obj}].
Datatype[{obj}].
Loaded Decoders[{QUEUE_DECODERS}].
https://bert-etl.readthedocs.io/en/latest/encoders_and_decoders.html''')

