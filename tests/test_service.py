import os
import pytest

import nb2workflow.workflow
from flask import url_for


@pytest.fixture
def app():
    app = nb2workflow.workflow.app
    print("creating app")
    return app

test_notebook=os.environ.get('TEST_NOTEBOOK')

def test_service(client):
    r=client.get('/api/options')

    print(r.json)

   # r=client.get('/')
   # assert r.status_code == 200

