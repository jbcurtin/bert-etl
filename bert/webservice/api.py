import enum
import functools
import typing

PWN: typing.TypeVar= typing.TypeVar('PWN')

class Methods(enum.Enum):
    Get: str = 'get'
    Post: str = 'post'
    Delete: str = 'delete'

class Route:
    route: str
    def __init__(self: PWN, route: str) -> None:
        self.route = route

class API:
    route: Route
    method: Methods
    def __init__(self: PWN, route: Route, method: Methods) -> None:
        self.route = route
        self.method = method

def build(
    method: Methods,
    route: Route):
    def _wrapper(func):
        func._api = API(route, method)
        @functools.wraps(func)
        def _func_wrapper(*args, **kwargs):
            return func()

        return _func_wrapper

    return _wrapper

