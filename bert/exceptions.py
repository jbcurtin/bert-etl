class BertException(Exception):
    pass

class SchemaRequired(BertException):
    pass

class EncoderLoadError(BertException):
    pass

