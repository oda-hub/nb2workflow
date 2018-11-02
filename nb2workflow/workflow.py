from __future__ import print_function

from flask import Flask, make_response, jsonify, request
from flask.json import JSONEncoder
from flask_cache import Cache

import os

from nb2workflow.nbadapter import NotebookAdapter

class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
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
    app.notebook_adapter=NotebookAdapter(os.environ.get("TEST_NOTEBOOK"))
    app.json_encoder = CustomJSONEncoder
    return app

app = create_app()


@app.route('/api/v1.0/get',methods=['GET'])
def workflow():
    issues = []

    interpreted_parameters = app.notebook_adapter.interpret_parameters(request.args)
    issues += interpreted_parameters['issues']

    if len(issues)>0:
        return make_response(jsonify(issues=issues), 400)
    else:
        app.notebook_adapter.execute(interpreted_parameters['request_parameters'])

        return jsonify(app.notebook_adapter.extract_output())

@app.route('/api/v1.0/parameters',methods=['GET'])
def workflow_parameters():
    return jsonify(dict(
                    output=None,parameters=app.notebook_adapter.extract_parameters()
                ))

@app.route('/health')
def healthcheck():
    issues=[]

    if len(issues)==0:
        return "all is ok!"
    else:
        return make_response(jsonify(issues=issues), 500)

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9191)

