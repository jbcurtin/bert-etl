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

class HTTPHandler(socketserver.BaseRequestHandler):
    def _send_response(self: PWN) -> None:
        work_queue, done_queue, ologger = utils.comm_binders(self._func)
        work_queue.put({
            'method': self._api.method.value,
            'route': self._api.route.route,
        })
        # import ipdb; ipdb.set_trace()
        self._func()

        # Response
        current_date: str = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        try:
            body: str = json.dumps(next(done_queue))
        except StopIteration:
            body: str = '{}'

        msg: str = f"""HTTP/1.1 200 OK
Date: {current_date}
Content-Type: application/json; charset=UTF-8
Content-Length: {len(body)}
Connection: close
Server: noop

{body}
"""
        logger.info(f'200 OK')
        self.request.sendall(msg.encode('utf-8'))

    def _invalid_method(self, invalid_method: str, valid_method: str) -> None:
        current_date: str = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        body: str = f'Invalid Method[{invalid_method}]. Only Valid Method[{valid_method}]'
        msg: str = f"""HTTP/1.1 405 Method Not Allowed
Date: {current_date}
Content-Type: text/html; charset=UTF-8
Content-Length: {len(body)}
Connection: close
Server: noop

{body}
"""
        logger.info(f'405 Method Not Allowed[{invalid_method}]')
        self.request.sendall(msg.encode('utf-8'))

    def _invalid_path(self, invalid_url: str, valid_url: str):
        current_date: str = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        body: str = f'Invalid URL[{invalid_url}]. Only Valid URL[{valid_url}]'
        msg: str = f"""HTTP/1.1 400 Bad Request
Date: {current_date}
Content-Type: text/html; charset=UTF-8
Content-Length: {len(body)}
Connection: close
Server: noop

{body}
"""
        logger.info(f'400 Bad Request[{invalid_url}]')
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
        headers, body = request.split(b'\r\n\r\n')
        headers = io.BytesIO(headers)
        headers.readline()
        headers = {key: value for key, value in parse_headers(headers).items()}
        method, path, protocol = request.split(b'\r\n', 1)[0].decode('utf-8').lower().split(' ')
        logger.info(f'Message Recieved[{method}:{path}]')
        if method == self._api.method.value:
            if path == self._api.route.route:
                self._send_response()

            else:
                self._invalid_path(path, self._api.route.route)

        else:
            self._invalid_method(method, self._api.method.value)

def serve_handler(api: api.API, func: types.FunctionType) -> None:
    WWW_HOST: str = os.environ.get('WWW_HOST', '127.0.0.1')
    try:
        WWW_PORT: int = int(os.environ.get('WWW_PORT', 8000))
    except ValueError:
        WWW_PORT = 8000

    HTTPHandler._api = api
    HTTPHandler._func = func
    with socketserver.TCPServer((WWW_HOST, WWW_PORT), HTTPHandler) as server:
        server.serve_forever()

