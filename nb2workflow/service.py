from __future__ import print_function

import os
import glob
import logging
import inspect
import requests
import base64

from io import BytesIO


from flask import Flask, make_response, jsonify, request, url_for, send_file
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

verify_tls = False

from nb2workflow.nbadapter import NotebookAdapter, find_notebooks
from nb2workflow import ontology, publish
    
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
    
    logger.debug("target %s",target)
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


        def funcg(target):
            def workflow_func():
                return workflow(target)
            return workflow_func

        logger.debug("target: %s with endpoint %s",target,endpoint)

        try:
            app.route('/api/v1.0/get/'+target,methods=['GET'],endpoint=endpoint)(
            swag_from(target_specs)(
            cache.cached(timeout=3600,key_prefix=make_key)(
                funcg(target)
            )))
        except AssertionError as e:
            logger.info("unable to add route:",e)
            raise

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

@app.route('/api/v1.0/get-file/<target>/<filename>',methods=['GET'])
def workflow_filename(target, filename):

    target_url = url_for('endpoint_'+target,_external=True,**request.args)

    # report equivalency

    r = requests.get(target_url, verify = verify_tls)
    
    try:
        output = r.json()['output']
        content = base64.b64decode(base64.b64decode(output.get(filename+'_content',None)))

        if content:
            return send_file(BytesIO(content), mimetype='image/png')
        else:
            return jsonify(dict(
                        exceptions=["no such file, available:",output.keys()],
                    ))

    except Exception as e:
        logger.error("problem decoding: %s",e)
        return jsonify(dict(
                    exceptions={"problem":r.content.decode('utf-8')},
                ))

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

@app.route('/')
def root():
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
    parser.add_argument('--publish', metavar='upstream-url', type=str, default=None)
    parser.add_argument('--publish-as', metavar='published url', type=str, default=None)
    parser.add_argument('--profile', metavar='service profile', type=str, default="oda")
    parser.add_argument('--debug', action="store_true")

    args = parser.parse_args()

    if args.debug:
        logging.getLogger("nb2workflow").setLevel(level=logging.DEBUG)
        logging.getLogger("flask").setLevel(level=logging.DEBUG)

    app.notebook_adapters = find_notebooks(args.notebook)
    setup_routes(app)
    app.service_semantic_signature=ontology.service_semantic_signature(app.notebook_adapters)

    if args.publish:
        logger.info("publishing to %s",args.publish)

        if args.publish_as:
            publish_host, publish_port = args.publish_as.split(":")
        else:
            publish_host, publish_port = args.host, args.port

        for nba_name, nba in app.notebook_adapters.items():
            publish.publish(args.publish, nba_name, publish_host, publish_port)


  #  for rule in app.url_map.iter_rules():
 #       logger.debug("==>> %s %s %s %s",rule,rule.endpoint,rule.__class__,rule.__dict__)

    app.run(host=args.host,port=args.port)

if __name__ == '__main__':
    main()

