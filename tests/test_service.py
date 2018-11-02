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
    app.notebook_adapter=nb2workflow.nbadapter.NotebookAdapter(os.environ.get("TEST_NOTEBOOK"))
    print("creating app")
    return app

test_notebook=os.environ.get('TEST_NOTEBOOK')

def test_service(client):
    r=client.get('/api/v1.0/options')
    
    service_signature=r.json['default']
    print(service_signature)

    assert len(service_signature['parameters'])==3

    r=client.get('/api/v1.0/get/default',query_string=dict(eminFAKE=20.))
    assert r.status_code == 400
    
    print(r.json['issues'])
    assert len(r.json['issues'])==1


    r=client.get('/api/v1.0/get/default',query_string=dict(emin=20.))
    assert r.status_code == 200

    print(r.json)

    open("output.png","w").write(base64.b64decode(r.json['spectrum']['content']))
