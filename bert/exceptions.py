class BertException(Exception):
    pass

class SchemaRequired(BertException):
    pass

class EncoderLoadError(BertException):
    pass

class BertEncoderError(BertException):
    pass

class BertDecoderError(BertException):
    pass

class BertConfigError(BertException):
    pass

class AWSError(BertException):
    pass

class BertIdentityEncoderError(BertException):
    pass
