from __future__ import print_function

import os
import pytest
import base64

import nb2workflow.service
import nb2workflow.nbadapter
from flask import url_for


@pytest.fixture
def app():
    app = nb2workflow.service.app
    app.notebook_adapters = nb2workflow.nbadapter.find_notebooks(os.environ.get("TEST_NOTEBOOK"))
    nb2workflow.service.setup_routes(app)
    print("creating app")
    return app

def test_service(client):
    r=client.get('/api/v1.0/options')
    
    service_name,service_signature=sorted(r.json.items())[0]
    print(service_signature)

    assert len(service_signature['parameters'])==4

    print('get: /api/v1.0/get/'+service_name)

    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(eminFAKE=20.))
    assert r.status_code == 400
    
    print(r.json['issues'])
    assert len(r.json['issues'])==1


    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(emin=20.))
    assert r.status_code == 200

    print(r.json)

    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))

# trace
    r=client.get('/api/v1.0/options')
    
    service_name,service_signature=sorted(r.json.items())[0]

    print('get: /api/v1.0/get/'+service_name)

    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(emin=20.))
    assert r.status_code == 200

    print(r.json)
    
    r=client.get('/trace/list')
    assert r.status_code == 200

    for l in sorted(r.json, key=lambda x:x['ctime']):
        print(l)

    job = r.json[-1]['fn'].split("/")[-1]
    
    r=client.get('/trace/'+job)

    print("job", r.json)

#    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))
