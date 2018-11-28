from __future__ import print_function

import os
import pytest
import base64

from imp import reload

from nb2workflow import service
import nb2workflow.nbadapter
from flask import url_for


@pytest.fixture
def app():
    reload(service)
    app = service.app
    app.notebook_adapters=nb2workflow.nbadapter.find_notebooks(os.environ.get("TEST_NOTEBOOK_REPO"))
    app.config["CACHE_TYPE"] = "null"
    service.cache.init_app(app)
    service.setup_routes(app)
    print("creating app")
    return app


def test_service(client):
    r=client.get('/api/v1.0/options')
    
    service_signature=r.json['workflow-notebook']
    print(service_signature)

    assert len(service_signature['parameters'])==4

    r=client.get('/api/v1.0/get/workflow-notebook',query_string=dict(eminFAKE=20.))
    assert r.status_code == 400
    
    print(r.json)
    print(r.json['issues'])
    assert len(r.json['issues'])==1


    r=client.get('/api/v1.0/get/workflow-notebook',query_string=dict(emin=20.))
    assert r.status_code == 200

    print(r.json)

    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))
