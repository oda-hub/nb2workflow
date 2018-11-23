from __future__ import print_function

import os
import glob
import logging

from flask import Flask, make_response, jsonify, request
from flask.json import JSONEncoder
from flask_caching import Cache
from flask_cors import CORS

from flasgger import LazyJSONEncoder, LazyString, Swagger

from nb2workflow.nbadapter import NotebookAdapter, find_notebooks

class ReverseProxied(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_FORWARDED_PREFIX', '')
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        return self.app(environ, start_response)


class CustomJSONEncoder(LazyJSONEncoder):
    def default(self, obj, *args, **kwargs):
        try:
            if isinstance(obj, type):
                return dict(type_object=repr(obj))
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)

cache = Cache(config={'CACHE_TYPE': 'simple'})

def create_app():
    app=Flask(__name__)
    swagger = Swagger(app, template=template)
    app.wsgi_app = ReverseProxied(app.wsgi_app)
    app.json_encoder = CustomJSONEncoder
    cache.init_app(app, config={'CACHE_TYPE': 'simple'})
#    CORS(app)
    return app


template = dict(swaggerUiPrefix=LazyString(lambda : request.environ.get('HTTP_X_FORWARDED_PREFIX', '')))

app = create_app()

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

def make_key():
    """Make a key that includes GET parameters."""
    return request.full_path

@app.route('/api/v1.0/get/<string:target>',methods=['GET'])
@cache.cached(timeout=3600,key_prefix=make_key)
def workflow(target):
    issues = []

    nba = app.notebook_adapters.get(target)

    if nba is None:
        issues.append("target not known: %s; available targets: %s"%(target,app.notebook_adapters.keys()))
    else:
        interpreted_parameters = nba.interpret_parameters(request.args)
        issues += interpreted_parameters['issues']

    if len(issues)>0:
        return make_response(jsonify(issues=issues), 400)
    else:
        nba.execute(interpreted_parameters['request_parameters'])

        return jsonify(dict(
                    output=nba.extract_output(),
                    exceptions=nba.exceptions,
                ))

# list input -> output function signatures and identities

@app.route('/api/v1.0/options',methods=['GET'])
def workflow_options():
    return jsonify(dict([
                    (
                        target,
                        dict(output=nba.extract_output_declarations(),parameters=nba.extract_parameters()),
                    )
                     for target, nba in app.notebook_adapters.items()
                    ]))

@app.route('/health')
def healthcheck():
    issues=[]

    if len(issues)==0:
        return "all is ok!"
    else:
        return make_response(jsonify(issues=issues), 500)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('--host', metavar='host', type=str, default="127.0.0.1")
    parser.add_argument('--port', metavar='port', type=int, default=9191)
    #parser.add_argument('--tmpdir', metavar='tmpdir', type=str, default=None)
    parser.add_argument('--upsteam', metavar='upstream-url', type=str, default="https://api.odahub.io/register")
    parser.add_argument('--profile', metavar='service profile', type=str, default="oda")
    parser.add_argument('--debug', action="store_true")

    args = parser.parse_args()

    app.notebook_adapters = find_notebooks(args.notebook)

    if args.debug:
        logger=logging.getLogger("nb2workflow")
        logger.setLevel(level=logging.DEBUG)

    app.run(host=args.host,port=args.port)

if __name__ == '__main__':
    main()

