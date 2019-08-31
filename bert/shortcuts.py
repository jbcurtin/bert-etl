import os

def getcwd() -> str:
    execution_env: str = os.environ.get('AWS_EXECUTION_ENV', None)
    if execution_env:
        return '/tmp'

    return os.getcwd()

