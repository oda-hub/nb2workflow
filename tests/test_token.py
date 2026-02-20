import logging
import threading
import time

logger = logging.getLogger(__name__)


def test_token_async(client):
    status_callback_file = "status.json"
    callback_url = 'file://' + status_callback_file
    token = 'abc123'
    query_string = dict(
        a=20,
        _async_request='yes',
        _async_request_callback=callback_url,
            _token=token)

    r = client.get('/api/v1.0/get/token',
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

        r = client.get('/api/v1.0/get/token',
                       query_string=query_string)

        logger.info('service returns %s %s', r, r.json)

        if r.json['workflow_status'] == 'done':
            logger.info('workflow done!')
            break

        time.sleep(0.1)

    test_worker_thread.join()
    assert 'data' in r.json
    assert 'output' in r.json['data']
    assert 'token' in r.json['data']['output']
    assert r.json['data']['output']['token'] == token


def test_token_sync(client):
    status_callback_file = "status.json"
    callback_url = 'file://' + status_callback_file
    token = 'abc123'
    query_string = dict(
        a=20,
        _token=token)

    r = client.get('/api/v1.0/get/token',
                   query_string=query_string)

    assert r.status_code == 200
    # the structure of sync response is different (no 'data' key)
    assert 'output' in r.json
    assert 'token' in r.json['output']
    assert r.json['output']['token'] == token


def test_token_no_access(client):
    nb_url = '/api/v1.0/get/token_no_access'
    status_callback_file = "status.json"
    callback_url = 'file://' + status_callback_file
    token = 'abc123'
    query_string = dict(
        a=20,
        _async_request='yes',
        _async_request_callback=callback_url,
            _token=token)

    r = client.get(nb_url,
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

        r = client.get(nb_url,
                       query_string=query_string)

        logger.info('service returns %s %s', r, r.json)

        if r.json['workflow_status'] == 'done':
            logger.info('workflow done!')
            break

        time.sleep(0.1)

    test_worker_thread.join()
    assert 'data' in r.json
    assert 'output' in r.json['data']
    assert 'token' in r.json['data']['output']
    assert r.json['data']['output']['token'] == 'undefined'
    