from __future__ import print_function
import os
import logging
import os
import pytest
import base64
from importlib import reload

from nb2workflow import service
import nb2workflow.nbadapter
from flask import url_for

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger("nb2workflow")
logger.setLevel(level=logging.DEBUG)

def test_workflow_localfile(test_notebook_repo):
    
    from nb2workflow import workflows

    result = workflows.evaluate("localfile", test_notebook_repo, "workflow-notebook")

    print(result)
    assert result['output']
    assert len(result['output']) == 6
    assert result['exceptions'] == []

    assert 'spectrum' in result['output']

@pytest.mark.xfail
def test_workflow_exception_localfile(test_notebook):
    
    from nb2workflow import workflows

    result = workflows.evaluate("localfile", test_notebook, "workflow-notebook", scwid="66500220010.001")

    print(result)
    assert len(result['output']) == 0
    assert len(result['exceptions']) == 1

    ex = result['exceptions'][0]

    print(ex)

    

@pytest.mark.service
def test_service(service_fixture, app):
    from nb2workflow import workflows

    result = workflows.evaluate("host", service_fixture, "workflow-notebook")

    print(result)
    assert result['output']
    assert len(result['output']) == 4
    assert result['exceptions'] == []

    assert 'spectrum' in result['output']

@pytest.mark.service
def test_workflow_exception_service(service_fixture, app):
    from nb2workflow import workflows

    result = workflows.evaluate("host", service_fixture, "workflow-notebook", scwid="66500220010.001")

    print(result)
    assert len(result['output']) == 0
    assert len(result['exceptions']) == 1

    ex = result['exceptions'][0]

    print(ex)

@pytest.mark.service
def test_async_service(service_fixture, app):
    from nb2workflow import workflows

    result = workflows.evaluate("host", service_fixture, "workflow-notebook", _async_request = True)

    print(result)
    assert result['output']
    assert len(result['output']) == 4
    assert result['exceptions'] == []

    assert 'spectrum' in result['output']

@pytest.mark.service
def test_async_service_exception(service_fixture, app):
    from nb2workflow import workflows

    result = workflows.evaluate("host", service_fixture, "workflow-notebook", _async_request = True, scwid="66500220010.001")

    print(result)
    assert len(result['output']) == 0
    assert len(result['exceptions']) == 1

    ex = result['exceptions'][0]

    print(ex)
