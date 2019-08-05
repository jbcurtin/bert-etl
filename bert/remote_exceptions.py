import flask
import json

from bert import exceptions

class BadRequest(exceptions.BertException):
  def __init__(self, message: str, errors: dict, status_code: int = 500) -> None:
    self.message = message
    self.errors = errors
    self.status_code = status_code
  def as_response(self) -> flask.Response:
    return flask.Response(json.dumps({'message': self.message, 'errors': self.errors}), self.status_code, mimetype='application/json')


