from __future__ import print_function

from flask import Flask, make_response, jsonify, request
from flask.json import JSONEncoder
from flask_caching import Cache


import os
import glob

from nb2workflow.nbadapter import NotebookAdapter

class CustomJSONEncoder(JSONEncoder):
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

def create_app():
    app=Flask(__name__)
    app.json_encoder = CustomJSONEncoder
    cache = Cache(app,config={'CACHE_TYPE': 'simple'})
    return app

app = create_app()


@app.route('/api/v1.0/get/<string:target>',methods=['GET'])
def workflow(target):
    if target!="default":
        return make_response(jsonify("currently only support default target"), 400)

    issues = []

    interpreted_parameters = app.notebook_adapter.interpret_parameters(request.args)
    issues += interpreted_parameters['issues']

    if len(issues)>0:
        return make_response(jsonify(issues=issues), 400)
    else:
        app.notebook_adapter.execute(interpreted_parameters['request_parameters'])

        return jsonify(app.notebook_adapter.extract_output())

# list input -> output function signatures and identities

@app.route('/api/v1.0/options',methods=['GET'])
def workflow_options():
    return jsonify(dict(
                    default=dict(
                        output=None,parameters=app.notebook_adapter.extract_parameters())
                  ))

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

    args = parser.parse_args()
    
    if os.path.isdir(args.notebook):
        notebooks=[ fn for fn in glob.glob(args.notebook+"/*ipynb") if "output" not in fn ]

        if len(notebooks)==0:
            raise Exception("no notebooks found in the directory:",args.notebook)

        if len(notebooks)>1:
            raise Exception("currently unable to handle many notebooks",notebooks)

        app.notebook_adapter=NotebookAdapter(notebooks[0])

    elif os.path.isfile(args.notebook):
        app.notebook_adapter=NotebookAdapter(args.notebook)

    else:
        raise Exception("requested notebook not found:",args.notebook)

    app.run(host=args.host,port=args.port)

if __name__ == '__main__':
    main()

