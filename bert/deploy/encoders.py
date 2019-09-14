import base64
import numpy as np

class NumpyIdentityEncoder(json.JSONEncoder):
    def default(self: PWN, obj: typing.Any) -> typing.Any:
        if isinstance(obj, (
            np.float32,
            np.float64,
            np.float16,
            np.complex32,
            np.complex64,
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            np.uint8,
            np.uint16,
            np.uint32,
            np.uint64,
            np.intc,
            np.intp,
            np.bool_,
            np.ndarray)):
            return base64.b64encode(obj)

        return super(NumpyIdentityEncoder, self).default(obj)

def numpy_done_queue_encoder(obj: typing.Any) -> str:
    import ipdb; ipdb.set_trace()
    pass

def numpy_work_queue_decoder(obj: str) -> typing.Any:
    import ipdb; ipdb.set_trace()
    pass

