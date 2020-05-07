#!python
  
import logging
import sys
import re
import os
import logging

from app import app

class ReverseProxied(object):
  def __init__(self, app):
    self.app = app

  def __call__(self, environ, start_response):
    script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
    if script_name:
      environ['SCRIPT_NAME'] = script_name
      path_info = environ['PATH_INFO']
      if path_info.startswith(script_name):
        environ['PATH_INFO'] = path_info[len(script_name):]

    scheme = environ.get('HTTP_X_FORWARDED_PROTO', '')
    if scheme:
      environ['wsgi.url_scheme'] = scheme
    return self.app(environ, start_response)

app.wsgi_app = ReverseProxied(app.wsgi_app)

root = logging.getLogger()
root.setLevel(logging.DEBUG)

sys.stdout = sys.stderr

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

if __name__ == "__main__":
    from flup.server.fcgi import WSGIServer
    WSGIServer(app).run()
