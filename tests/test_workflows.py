from __future__ import print_function
import os
import logging
import os
import pytest
import base64
from imp import reload

from nb2workflow import service
import nb2workflow.nbadapter
from flask import url_for

test_notebook=os.environ.get('TEST_NOTEBOOK')
test_notebook_repo=os.environ.get('TEST_NOTEBOOK_REPO')

#logger=logging.getLogger("nb2workflow")

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger("nb2workflow")
logger.setLevel(level=logging.DEBUG)

def test_workflow_localfile():
    
    from nb2workflow import workflows

    result = workflows.evaluate("localfile", os.environ.get("TEST_NOTEBOOK_REPO"), "workflow-notebook")

    print(result)
    assert result['output']
    assert len(result['output']) == 4
    assert result['exceptions'] == []

    assert 'spectrum' in result['output']
    

def test_service(service_fixture, app):
    from nb2workflow import workflows

    result = workflows.evaluate("host", service_fixture, "workflow-notebook")

    print(result)
    assert result['output']
    assert len(result['output']) == 4
    assert result['exceptions'] == []

    assert 'spectrum' in result['output']
