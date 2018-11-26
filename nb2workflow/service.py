from __future__ import print_function

import os
import glob
import logging
import inspect


from flask import Flask, make_response, jsonify, request
from flask.json import JSONEncoder
from flask_caching import Cache
from flask_cors import CORS

from flasgger import LazyJSONEncoder, LazyString, Swagger, swag_from

from logging.config import dictConfig

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
  #      'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

from nb2workflow.nbadapter import NotebookAdapter, find_notebooks
from nb2workflow import ontology
    
logger=logging.getLogger('nb2workflow.service')

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
    template = dict(swaggerUiPrefix=LazyString(lambda : request.environ.get('HTTP_X_FORWARDED_PREFIX', '')))
    swagger = Swagger(app, template=template)
    app.wsgi_app = ReverseProxied(app.wsgi_app)
    app.json_encoder = CustomJSONEncoder
    cache.init_app(app, config={'CACHE_TYPE': 'simple'})
#    CORS(app)
    return app


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


def workflow(target):
    issues = []
    
    logger.debug("raw parameters %s",request.args)

    nba = app.notebook_adapters.get(target)

    if nba is None:
        issues.append("target not known: %s; available targets: %s"%(target,app.notebook_adapters.keys()))
    else:
        interpreted_parameters = nba.interpret_parameters(request.args)
        issues += interpreted_parameters['issues']

    logger.debug("interpreted parameters %s",interpreted_parameters)

    if len(issues)>0:
        return make_response(jsonify(issues=issues), 400)
    else:
        nba.execute(interpreted_parameters['request_parameters'])

        output=nba.extract_output()

        logger.debug("output: %s",output)
        logger.debug("exceptions: %s",nba.exceptions)

        return jsonify(dict(
                    output=output,
                    exceptions=[repr(e) for e in nba.exceptions],
                ))

def to_oapi_type(in_type):
    out_type='string'

    if issubclass(in_type,int):
        out_type='integer'

    if issubclass(in_type,float):
        out_type='number'
    
    if issubclass(in_type,str):
        out_type='string'
    
    logger.debug("oapi type cast from %s to %s",repr(in_type),repr(out_type))
    
    return out_type

def setup_routes(app):
    for target, nba in app.notebook_adapters.items():
        target_specs=specs_dict = {
              "parameters": [
                {
                  "name": p_name,
                  "in": "query",
                  "type": to_oapi_type(p_data['python_type']),
                  "required": "false",
                  "default": p_data['default_value'],
                  "description": p_data['comment']+" "+p_data['owl_type'],
                }
                for p_name, p_data in nba.extract_parameters().items()
              ],
              "responses": {
                "200": {
                  "description": repr(nba.extract_output_declarations()),
                }
              }
            }

        endpoint='endpoint_'+target
        try:
            app.route('/api/v1.0/get/'+target,methods=['GET'],endpoint=endpoint)(
            swag_from(target_specs)(
            cache.cached(timeout=3600,key_prefix=make_key)(
                lambda :workflow(target)
            )))
        except AssertionError as e:
            logger.info("unable to add route:",e)

#    app.route('/api/v1.0/get/<target>',methods=['GET'],endpoint='endpoint_undefined')(
#    swag_from(target_specs)(
#    cache.cached(timeout=3600,key_prefix=make_key)(
#        workflow
#    )))

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

@app.route('/api/v1.0/rdf',methods=['GET'])
def workflow_rdf():
    return make_response(app.service_semantic_signature)

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
    setup_routes(app)
    app.service_semantic_signature=ontology.service_semantic_signature(app.notebook_adapters)

    if args.debug:
        logging.getLogger("nb2workflow").setLevel(level=logging.DEBUG)
        logging.getLogger("flask").setLevel(level=logging.DEBUG)


    app.run(host=args.host,port=args.port)

if __name__ == '__main__':
    main()

