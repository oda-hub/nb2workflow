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
    print("creating app")
    return app

def test_service(client):
    r=client.get('/api/v1.0/options')
    
    service_name,service_signature=r.json.items()[0]
    print(service_signature)

    assert len(service_signature['parameters'])==4

    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(eminFAKE=20.))
    assert r.status_code == 400
    
    print(r.json['issues'])
    assert len(r.json['issues'])==1


    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(emin=20.))
    assert r.status_code == 200

    print(r.json)

    open("output.png","w").write(base64.b64decode(r.json['spectrum_png_content']))
