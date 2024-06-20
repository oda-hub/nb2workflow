import pytest
import nb2workflow
import logging
import threading
import time
import os
import json

logger = logging.getLogger(__name__)
status_callback_file = "status.json"

@pytest.fixture
def app():
    testfiles_path = os.path.join(os.path.dirname(__file__), 'testfiles')
    app = nb2workflow.service.app
    app.notebook_adapters = nb2workflow.nbadapter.find_notebooks(testfiles_path)
    nb2workflow.service.setup_routes(app)
    print("creating app")
    return app


def test_progress_callback(client):
    callback_url = 'file://' + status_callback_file
    query_string = dict(
        a=20,
        _async_request='yes',
        _async_request_callback=callback_url)

    r = client.get('/api/v1.0/get/callback',
                   query_string=query_string)

    assert r.status_code == 201

    logger.info(r.json)

    from nb2workflow.service import AsyncWorker

    def test_worker_run():
        AsyncWorker('test-worker').run_one()

    test_worker_thread = threading.Thread(target=test_worker_run)
    test_worker_thread.start()

    while True:
        options = client.get('/api/v1.0/options')
        assert options.status_code == 200

        r = client.get('/api/v1.0/get/callback',
                       query_string=query_string)

        logger.info('service returns %s %s', r, r.json)

        if r.json['workflow_status'] == 'done':
            logger.info('workflow done!')
            break

        time.sleep(0.1)

    test_worker_thread.join()
    assert 'data' in r.json
    assert 'output' in r.json['data']
    assert 'callback' in r.json['data']['output']
    assert r.json['data']['output']['callback'] == callback_url

    workdir = r.json['data']['jobdir']
    with open(os.path.join(workdir, status_callback_file)) as json_file:
        progress_params = json.load(json_file)

    test_data = dict(action='progress', 
                     stage='simulation', 
                     progress=50, 
                     substage='spectra', 
                     subprogress=30, 
                     message='some message',
                     progress_max=100.0, 
                     subprogress_max=100.0)
    assert progress_params == test_data

