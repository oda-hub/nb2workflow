import pytest
import nb2workflow
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

@pytest.fixture
def app():
    testfiles_path = os.path.join(os.path.dirname(__file__), 'testfiles')
    app = nb2workflow.service.app
    app.notebook_adapters = nb2workflow.nbadapter.find_notebooks(testfiles_path)
    nb2workflow.service.setup_routes(app)
    print("creating app")
    return app

@pytest.mark.parametrize('exc_type', ['runtime', 'kernel'])
def test_exceptions_sync(client, exc_type):
    r = client.get(f'/api/v1.0/get/raising?exception_type={exc_type}')
    
    assert "exceptions" in r.json
    assert len(r.json['exceptions']) > 0 

@pytest.mark.parametrize('exc_type', ['runtime', 'kernel'])
def test_exceptions_async(client, exc_type):

    r = client.get(
        f'/api/v1.0/get/raising?exception_type={exc_type}&_async_request=True'
        )
    assert r.status_code == 201

    from nb2workflow.service import AsyncWorker

    def test_worker_run():
        AsyncWorker('test-worker').run_one()

    test_worker_thread = threading.Thread(target=test_worker_run)
    # test_worker_thread = AsyncWorker('test-worker')
    test_worker_thread.start()
    
    while True:
        r = client.get(
            f'/api/v1.0/get/raising?exception_type={exc_type}&_async_request=True'
            )
        if r.status_code == 201:
            time.sleep(0.1)
            continue
        
        assert r.status_code == 200
        assert 'jobdir' in r.json['data']
        assert len(r.json['data']['exceptions']) > 0
        break

    test_worker_thread.join()
