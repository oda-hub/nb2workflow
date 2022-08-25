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

    for l in sorted(r.json, key=lambda x:x['ctime']):
        logger.info(l)

    job = r.json[-1]['fn'].split("/")[-1]
    
    r=client.get('/trace/'+job)

    logger.info("job %s", r.json)

#    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))



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
        if os.path.exists(callback_fn):
            callback_json = json.load(open(callback_fn))
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

        if r.json['workflow_status'] == 'done':
            logger.info('workflow done!')
            break

        time.sleep(0.1)

    test_worker_thread.join()

    open("output.png","wb").write(base64.b64decode(r.json['data']['output']['spectrum_png_content']))
