import pytest
import nb2workflow
import logging
import threading
import time

logger = logging.getLogger(__name__)

@pytest.fixture
def app():
    app = nb2workflow.service.app
    app.notebook_adapters = nb2workflow.nbadapter.find_notebooks('tests/testfiles')
    nb2workflow.service.setup_routes(app)
    print("creating app")
    return app


def test_progress_callback(client):
    callback_url = 'file://callback.json'
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
    assert r.json['data']['output']['callback'] == callback_url