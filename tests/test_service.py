from __future__ import print_function

import os
import json
import pytest
import base64

import nb2workflow.service
import nb2workflow.nbadapter
from flask import url_for




def test_service(client):
    r=client.get('/api/v1.0/options')
    
    service_name,service_signature=sorted(r.json.items())[0]
    print(service_signature)

    assert len(service_signature['parameters']) == 5

    #TODO: assert here paremeters

    print('get: /api/v1.0/get/'+service_name)

    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(eminFAKE=20.))
    assert r.status_code == 400
    
    print(r.json['issues'])
    assert len(r.json['issues'])==1


    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(emin=20.))
    assert r.status_code == 200

    print(r.json)

    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))

# trace
    r=client.get('/api/v1.0/options')
    
    service_name,service_signature=sorted(r.json.items())[0]

    print('get: /api/v1.0/get/'+service_name)

    r=client.get('/api/v1.0/get/'+service_name,query_string=dict(emin=20.))
    assert r.status_code == 200

    print(r.json)
    
    r=client.get('/trace/list')
    assert r.status_code == 200

    for l in sorted(r.json, key=lambda x:x['ctime']):
        print(l)

    job = r.json[-1]['fn'].split("/")[-1]
    
    r=client.get('/trace/'+job)

    print("job", r.json)

#    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))



def test_service_repo(client):
    
    r=client.get('/api/v1.0/options')
    
    service_signature=r.json['workflow-notebook']
    print(service_signature)

    assert len(service_signature['parameters']) == 5

    r=client.get('/api/v1.0/get/workflow-notebook',query_string=dict(eminFAKE=20.))
    assert r.status_code == 400
    
    print(r.json)
    print(r.json['issues'])
    assert len(r.json['issues'])==1


    r=client.get('/api/v1.0/get/workflow-notebook',query_string=dict(emin=20.))
    assert r.status_code == 200

    print(r.json)

    open("output.png","wb").write(base64.b64decode(r.json['output']['spectrum_png_content']))



def test_service_async_repo(client):
    
    r=client.get('/api/v1.0/options')
    
    service_signature=r.json['workflow-notebook']
    print(service_signature)

    assert len(service_signature['parameters']) == 5

    callback_fn = 'callback.json'
    
    r=client.get('/api/v1.0/get/workflow-notebook',
                 query_string=dict(
                     emin=20., 
                     _async_request='yes', 
                     _async_request_callback='file://' + callback_fn))

    assert r.status_code == 201

    print(r.json)

    from nb2workflow.service import AsyncWorker

    AsyncWorker('test-worker').run_one()

    callback_json = json.load(open(callback_fn))

    assert callback_json['action'] == 'done'

    r=client.get('/api/v1.0/get/workflow-notebook',
                query_string=dict(
                    emin=20., 
                    _async_request='yes', 
                    _async_request_callback='file://' + callback_fn))

    assert r.json['workflow_status'] == 'done'

    open("output.png","wb").write(base64.b64decode(r.json['data']['output']['spectrum_png_content']))
