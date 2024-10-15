from __future__ import print_function

import json
import os
import threading
import base64
import time


from flask import url_for

import logging

logger = logging.getLogger(__name__)

def test_service(client):
    r=client.get('/api/v1.0/options')
    
    service_name,service_signature=sorted(r.json.items())[0]
    logger.info(service_signature)

    assert len(service_signature['parameters']) == 6

    #TODO: assert here paremeters

    logger.info('get: /api/v1.0/get/'+service_name)

    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(eminFAKE=20.))
    assert r.status_code == 400
    
    logger.info(r.json['issues'])
    assert len(r.json['issues'])==1


    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(emin=20.))
    assert r.status_code == 200

    logger.info(r.json)

    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))

# trace
    r=client.get('/api/v1.0/options')
    
    service_name,service_signature=sorted(r.json.items())[0]

    logger.info('get: /api/v1.0/get/'+service_name)

    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(emin=20.))
    assert r.status_code == 200

    logger.info(r.json)
    
    r=client.get('/trace/list')
    assert r.status_code == 200

    sorted_json=sorted(r.json, key=lambda x:x['ctime'])
    for l in sorted_json:
        logger.info(l)

    job = sorted_json[-1]['fn'].split("/")[-1]

    logger.info("job %s", job)

    r=client.get('/trace/'+job)

    logger.info("r.json %s", r.json)
    print("r.json ", r.json)

#    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))
    service_name = r.json[0].split("/")[-1].replace('_output.ipynb', '')
    r = client.get(os.path.join('trace', job, service_name),
                   query_string=dict(include_glued_output=True))

    html_output = r.data.decode()
    assert "celltag_injected-gather-outputs" in html_output

    r = client.get(os.path.join('trace', job, service_name),
                   query_string=dict(include_glued_output=False))

    html_output = r.data.decode()
    assert "celltag_injected-gather-outputs" not in html_output
    assert "<title>500 Internal Server Error</title>" not in html_output


def test_service_repo(client):
    
    r=client.get('/api/v1.0/options')
    
    service_signature=r.json['workflow-notebook']
    logger.info(service_signature)

    assert len(service_signature['parameters']) == 6

    r=client.get('/api/v1.0/get/workflow-notebook',query_string=dict(eminFAKE=20.))
    assert r.status_code == 400
    
    logger.info(r.json)
    logger.info(r.json['issues'])
    assert len(r.json['issues'])==1


    r=client.get('/api/v1.0/get/workflow-notebook',query_string=dict(emin=20.))
    assert r.status_code == 200

    logger.info(r.json)

    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))



def test_service_async_repo(client):
    thread_id = threading.get_ident()
    process_id = os.getpid()
    logger.info(f'test_service_async_repo thread id: {thread_id} ; process id: {process_id}')

    r = client.get('/api/v1.0/options')
    
    service_signature=r.json['workflow-notebook']
    logger.info(service_signature)

    assert len(service_signature['parameters']) == 6

    callback_fn = 'callback.json'
    
    r=client.get('/api/v1.0/get/workflow-notebook',
                 query_string=dict(
                     emin=20., 
                     _async_request='yes', 
                     _async_request_callback='file://' + callback_fn))

    assert r.status_code == 201

    logger.info(r.json)

    from nb2workflow.service import AsyncWorker

    def test_worker_run():
        AsyncWorker('test-worker').run_one()

    test_worker_thread = threading.Thread(target=test_worker_run)
    test_worker_thread.start()



    while True:
        # if os.path.exists(callback_fn):
        #     callback_json = json.load(open(callback_fn))
            # assert callback_json['action'] == 'done'
        
        options = client.get('/api/v1.0/options')
        logger.info('\033[31moptions returns %s %s\033[0m', options, options.json)
        assert options.status_code == 200
    
        r = client.get('/api/v1.0/get/workflow-notebook',
                    query_string=dict(
                        emin=20., 
                        _async_request='yes', 
                        _async_request_callback='file://' + callback_fn))

        logger.info('service returns %s %s', r, r.json)

        if r.json['workflow_status'] == 'started':
            assert 'jobdir' in r.json
            logger.info('jobdir is reported as %s', r.json['jobdir'])
        
        if r.json['workflow_status'] == 'done':
            logger.info('workflow done!')
            break

        time.sleep(0.1)

    test_worker_thread.join()

    logger.info("output has keys: %s", list(r.json['data']['output']))
        
    open("output.png","wb").write(base64.b64decode(r.json['data']['output']['spectrum_png_content']))
    
