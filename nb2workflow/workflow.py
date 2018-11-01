from __future__ import print_function

from flask import Flask, make_response, jsonify, request
from flask.json import JSONEncoder
from flask_cache import Cache



import os
import random
import subprocess
import pandas as pd

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

@app.route('/api/get',methods=['GET'])
def workflow():
    print("cmd:",cmd)

    output=subprocess.check_output(cmd,stderr=subprocess.STDOUT)

    lc=pd.read_csv(lc_fn)

    return jsonify(output=output,data=lc.to_json())

@app.route('/api/options',methods=['GET'])
def workflow_parameters():
    return jsonify(dict(
                    output=None,parameters=app.notebook_adapter.extract_parameters()
                ))

@app.route('/health')
def healthcheck():
    issues=[]

 #   if not os.path.exists(os.environ.get('POLAR_AUX')):
 #       issues.append("no POLAR_AUX directory")

    if len(issues)==0:
        return "all is ok!"
    else:
        return make_response(jsonify(issues=issues), 500)

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=9191)



