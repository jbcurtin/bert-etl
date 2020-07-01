from bert import exceptions as bert_exceptions

class ExampleException(bert_exceptions.BertException):
    pass

class ProjectNameRequiredException(ExampleException):
    pass

class DirectoryExistsException(ExampleException):
    pass

class ProjectRepoInvalidFormatException(ExampleException):
    pass

