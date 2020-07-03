import io
import logging
import json
import os
import socket
import socketserver
import types
import typing

from bert import utils
from bert.webservice import api

from datetime import datetime

from http.client import parse_headers

logger = logging.getLogger(__name__)
PWN: typing.TypeVar = typing.TypeVar('PWN')
ENCODING = 'utf-8'

class Status:
    OK: str = '200 OK'
    METHOD_NOT_ALLOWED: str = '405 Method Not Allowed'
    BAD_REQUEST: str = '400 Bad Request'

class ResponseType:
    def __init__(self: PWN, status: Status, body: typing.Union[bytes, str]) -> None:
        self._body = body
        self._status = status

    def render(self: PWN) -> bytes:
        headers = {}
        headers['Date'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        headers['Connection'] = 'close'
        headers['Server'] = 'bert-etl-dev-server'
        if not self._status is Status.OK:
            body = json.dumps(self._body)
            headers['Content-Type'] = 'text/html; charset=UTF-8'
            headers['Content-Length'] = '0'
            headers = '\n'.join([': '.join([key, value]) for key, value in headers.items()])
            return f"""HTTP/1.1 {self._status.value}\n{headers}\n\r{body}""".encode(ENCODING)

        elif isinstance(self._body, dict) and self._status is Status.OK:
            body = json.dumps(self._body)
            headers['Content-Type'] = 'application/json; charset={ENCODING}'
            headers['Content-Length'] = str(len(body))
            headers = '\n'.join([': '.join([key, value]) for key, value in headers.items()])
            try:
                return f"""HTTP/1.1 {self._status.value}\n{headers}\n\n{body}""".encode(ENCODING)
            except AttributeError:
                return f"""HTTP/1.1 {self._status}\n{headers}\n\n{body}""".encode(ENCODING)

        else:
            raise NotImplementedError


class HTTPHandler(socketserver.BaseRequestHandler):
    def _send_get_response(self: PWN) -> None:
        work_queue, done_queue, ologger = utils.comm_binders(self._func)
        work_queue.put({
            'method': self._api.method.value,
            'route': self._api.route.route,
        })
        func_response = self._func()
        stream_response = ResponseType(Status.OK, func_response)
        self.request.sendall(stream_response.render())


    def _invalid_method(self, invalid_method: str, valid_method: str) -> None:
        logger.info(f'405 Method Not Allowed[{invalid_method}]')
        body: str = f'Invalid Method[{invalid_method}]. Valid Method[{valid_method}]'
        stream_response = ResponseType(Status.METHOD_NOT_ALLOWED, body)
        self.request.sendall(stream_response.render())

    def _invalid_path(self, invalid_url: str, valid_url: str):
        logger.info(f'400 Bad Request[{invalid_url}]')
        body: str = f'Invalid URL[{invalid_url}]. Only Valid URL[{valid_url}]'
        stream_response = ResponseType(Status.BAD_REQUEST, body)
        self.request.sendall(stream_response.render())
        self.request.sendall(msg.encode('utf-8'))

    def handle(self):
        self.request.settimeout(1)
        data = []
        while True:
            try:
                datum = self.request.recv(1024)
            except socket.timeout:
                break
            else:
                data.append(datum)

        request = b''.join(data)
        self._handle_request(request)

    def _handle_request(self, request: bytes) -> None:
        headers, body = request.split(b'\r\n\r\n', 1)
        headers = io.BytesIO(headers)
        headers.readline()
        headers = {key: value for key, value in parse_headers(headers).items()}
        method, path, protocol = request.split(b'\r\n', 1)[0].decode(ENCODING).lower().split(' ')
        logger.info(f'Message Recieved[{method}:{path}]')
        if method == self._api.method.value:
            full_path: str = f'/{self._stage}{self._api.route.route}'
            if path == full_path:
                self._begin_response(method, path, protocol, headers, body)

            else:
                self._invalid_path(path, self._api.route.route)

        else:
            self._invalid_method(method, self._api.method.value)

    def _send_post_response(self: PWN, path, headers, body) -> None:
        if not headers['Content-Type'].startswith('multipart/form-data'):
            raise NotImplementedError

        post_contents = {}
        content_type, rest = headers['Content-Type'].encode(ENCODING).split(b';', 1)
        properties = dict([elm.strip().split(b'=') for elm in rest.split(b';')])
        properties = {key.decode(ENCODING): value.decode(ENCODING) for key, value in properties.items()}
        boundary = f'--{properties["boundary"]}'
        # We'll handle Binary data another day
        for part in [p for p in body.decode(ENCODING).split(boundary) if p]:
            if part == '--\r\n':
                continue

            part_headers, part_body = part.strip('\r\n').split('\r\n\r\n')
            part_headers = dict([elm.strip().split(':') for elm in part_headers.split('\n')])
            for key, value in part_headers.copy().items():
                if key == 'Content-Disposition':
                    if value.strip().startswith('form-data'):
                        _, properties = value.split(';', 1)
                        properties = dict([prop.strip().split('=') for prop in properties.strip().split(';')])
                        properties = {key.strip('"'): value.strip('"') for key, value in properties.items()}
                        if not 'name' in properties.keys():
                            raise NotImplementedError

                        if 'filename' in properties.keys():
                            post_contents[properties['name']] = {
                                'content-type': part_headers['Content-Type'],
                                'content': part_body
                            }

                        elif len(properties.keys()) == 1:
                            post_contents[properties['name']] = part_body

                elif key == 'Content-Type':
                    continue

                else:
                    raise NotImplementedError

        # Interop with bert-etl API
        work_queue, done_queue, ologger = utils.comm_binders(self._func)
        work_queue.put({
            'method': self._api.method.value,
            'route': self._api.route.route,
            'post-contents': post_contents
        })
        func_response = self._func()
        stream_response = ResponseType(Status.OK, func_response)
        self.request.sendall(stream_response.render())

    def _begin_response(self: PWN, method: str, path: str, protocol: str, headers: typing.Dict[str, str], body: str) -> None:
        if method.lower() == 'get':
            self._send_get_response()

        elif method.lower() == 'post':
            self._send_post_response(path, headers, body)

        else:
            raise NotImplementedError

def serve_handler(api: api.API, func: types.FunctionType, stage: str) -> None:
    WWW_HOST: str = os.environ.get('WWW_HOST', '127.0.0.1')
    try:
        WWW_PORT: int = int(os.environ.get('WWW_PORT', 8000))
    except ValueError:
        WWW_PORT = 8000

    HTTPHandler._api = api
    HTTPHandler._func = func
    HTTPHandler._stage = stage
    logger.info(f'Routing URL[{api.method}: /{stage}{api.route.route}]')
    with socketserver.TCPServer((WWW_HOST, WWW_PORT), HTTPHandler) as server:
        server.serve_forever()

