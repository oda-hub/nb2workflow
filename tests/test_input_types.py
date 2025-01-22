import pytest
import nb2workflow
import os
import logging
import threading
import time

logger = logging.getLogger(__name__)

from urllib.parse import urlencode, urlparse, parse_qs


@pytest.fixture
def app():
    testfiles_path = os.path.join(os.path.dirname(__file__), 'testfiles')
    app = nb2workflow.service.app
    app.notebook_adapters = nb2workflow.nbadapter.find_notebooks(testfiles_path)
    nb2workflow.service.setup_routes(app)
    nb2workflow.nbadapter.ontology._is_ontology_available = True
    print("creating app")
    return app

def test_posix_download_file(client):
    r = client.get('/api/v1.0/get/testposixpath')
    assert r.json['output']['output_file_download'] == 'file not downloaded'

def test_posix_download_file_no_ontology(client, app_not_available_ontology):
    r = client.get('/api/v1.0/get/testposixpath', query_string={'fits_file_path': 'https://fits.gsfc.nasa.gov/samples/testkeys.fits'})
    assert r.json['output']['output_file_download'] == 'file downloaded successfully'

def test_posix_download_file_no_ontology_extra_annotations(client, app_not_available_ontology):
    r = client.get('/api/v1.0/get/testposixpath_extra_annotated', query_string={'fits_file_path': 'https://fits.gsfc.nasa.gov/samples/testkeys.fits'})
    assert r.json['output']['output_file_download'] == 'file downloaded successfully'

def test_posix_download_file_with_arg(client):
    r = client.get('/api/v1.0/get/testposixpath', query_string={'fits_file_path': 'https://fits.gsfc.nasa.gov/samples/testkeys.fits'})
    assert r.json['output']['output_file_download'] == 'file downloaded successfully'

@pytest.mark.parametrize('arg', ('', '\x00'))
def test_posix_nonoptional_empty_arg(client, arg):
    r = client.get('/api/v1.0/get/testposixpath', query_string={'fits_file_path': arg})
    #assert r.json['output']['output_file_download'] == 'file not downloaded'
    assert r.status_code >= 400
    assert 'fits_file_path' in r.json['issues'][0]

def test_posix_optional_empty_string(client):
    r = client.get('/api/v1.0/get/testposixpath_optional', query_string={'fits_file_path': ''})
    #assert r.json['output']['output_file_download'] == 'file not downloaded'
    assert r.status_code >= 400
    assert 'Empty string is not a valid value of FileReference parameter fits_file_path' in r.json['issues'][0]

def test_posix_optional_none(client):
    r = client.get('/api/v1.0/get/testposixpath_optional', query_string={'fits_file_path': '\x00'})
    #assert r.json['output']['output_file_download'] == 'file not downloaded'
    assert r.status_code == 200
    assert r.json['output']['output_file_download'] == 'file path is not provided'

def test_posix_download_file_extra_annotations(client):
    r = client.get('/api/v1.0/get/testposixpath_extra_annotated', query_string={'fits_file_path': 'https://fits.gsfc.nasa.gov/samples/testkeys.fits'})
    assert r.json['output']['output_file_download'] == 'file downloaded successfully'



@pytest.mark.parametrize("query_string_fits_file_path", ["generic_url", "mmoda_url", None])
def test_file_reference(client, query_string_fits_file_path):
    status_callback_file = "status.json"
    callback_url = 'file://' + status_callback_file
    token = 'abc123'
    query_string = dict(
        _async_request='no',
        _async_request_callback=callback_url,
        _token=token)

    fits_file_path_value = None
    if query_string_fits_file_path == "generic_url":
        fits_file_path_value = "https://fits.gsfc.nasa.gov/samples/testkeys.fits"
    elif query_string_fits_file_path == "mmoda_url":
        fits_file_path_value = "https://www.astro.unige.ch/mmoda/dispatch-data/test.fits"

    query_string['fits_file_path'] = fits_file_path_value

    r = client.get(f'/api/v1.0/get/testfilereference_extra_annotated', query_string=query_string)
    assert r.status_code == 201

    from nb2workflow.service import AsyncWorker

    def test_worker_run():
        AsyncWorker('test-worker').run_one()

    test_worker_thread = threading.Thread(target=test_worker_run)
    test_worker_thread.start()

    while True:
        options = client.get('/api/v1.0/options')
        assert options.status_code == 200

        r = client.get(f'/api/v1.0/get/testfilereference_extra_annotated',
                       query_string=query_string)

        logger.info('service returns %s %s', r, r.json)

        if r.json['workflow_status'] == 'done':
            logger.info('workflow done!')
            break

        time.sleep(0.1)

    test_worker_thread.join()
    assert 'data' in r.json
    assert 'output' in r.json['data']
    assert 'fits_file_path_modified' in r.json['data']['output']

    if query_string_fits_file_path is None:
        assert r.json['data']['output']['fits_file_path_modified'] == "/home/local/test.fits"
    elif query_string_fits_file_path == "mmoda_url":
        url_parts = urlparse(r.json['data']['output']['fits_file_path_modified'])
        url_args = parse_qs(url_parts.query)
        assert 'token' in url_args
    elif query_string_fits_file_path == "generic_url":
        url_parts = urlparse(r.json['data']['output']['fits_file_path_modified'])
        url_args = parse_qs(url_parts.query)
        assert 'token' not in url_args

@pytest.mark.parametrize("query_string_fits_file_url", ["generic_url", "file_path", "numeric", None])
def test_mmoda_file_url(client, query_string_fits_file_url):
    status_callback_file = "status.json"
    callback_url = 'file://' + status_callback_file
    token = 'abc123'
    query_string = dict(
        _async_request='no',
        _async_request_callback=callback_url,
        _token=token)

    fits_file_url_value = None
    if query_string_fits_file_url == "generic_url":
        fits_file_url_value = "https://fits.gsfc.nasa.gov/samples/testkeys.fits"
    elif query_string_fits_file_url == "file_path":
        fits_file_url_value = "/home/local/test.fits"
    elif query_string_fits_file_url == "numeric":
        fits_file_url_value = 123456

    query_string['fits_file_url'] = fits_file_url_value

    r = client.get(f'/api/v1.0/get/testfileurl_extra_annotated', query_string=query_string)

    assert r.status_code == 201

    from nb2workflow.service import AsyncWorker

    def test_worker_run():
        AsyncWorker('test-worker').run_one()

    test_worker_thread = threading.Thread(target=test_worker_run)
    test_worker_thread.start()

    while True:
        options = client.get('/api/v1.0/options')
        assert options.status_code == 200

        r = client.get(f'/api/v1.0/get/testfileurl_extra_annotated',
                       query_string=query_string)

        logger.info('service returns %s %s', r, r.json)

        if r.json['workflow_status'] == 'done':
            logger.info('workflow done!')
            break

        time.sleep(0.1)

    test_worker_thread.join()
    assert 'data' in r.json
    assert 'output' in r.json['data']
    if query_string_fits_file_url != "file_path" and query_string_fits_file_url != "numeric":
        assert 'mmoda_url_modified' in r.json['data']['output']
        url_parts = urlparse(r.json['data']['output']['mmoda_url_modified'])
        url_args = parse_qs(url_parts.query)
        assert 'token' in url_args

        assert 'fits_file_url_modified' in r.json['data']['output']
        url_parts = urlparse(r.json['data']['output']['fits_file_url_modified'])
        url_args = parse_qs(url_parts.query)
        assert 'token' not in url_args

    else:
        assert r.json['data']['output'] == 'incomplete'
        assert r.json['data']['exceptions'][0]['edump'] == f"ValueError('Parameter fits_file_url value \"{fits_file_url_value}\" can not be interpreted as FileURL.')"

def test_posix_download_file_with_arg_low_download_limit(client, app_low_download_limit):
    r = client.get('/api/v1.0/get/testposixpath', query_string={'fits_file_path': 'https://fits.gsfc.nasa.gov/samples/testkeys.fits'})
    assert r.json['output'] == {}
    assert r.json['exceptions'][0] == "Exception('The file appears to be too large to download, and the download limit is set to 1 bytes.')"

def test_posix_download_file_with_arg_wrong_url(client):
    r = client.get('/api/v1.0/get/testposixpath', query_string={'fits_file_path': 'https://fits.gsfc.nasa.gov/samples/aaaaaa.fits'})
    assert r.json['exceptions'][0] == ("Exception('An issue, due to invalid status code, occurred when attempting to getting the file size at the url "
                                       "https://fits.gsfc.nasa.gov/samples/aaaaaa.fits. This might be related "
                                       "to an invalid url, please check the input provided')")

@pytest.mark.parametrize("public", [True, False])
@pytest.mark.parametrize("mmoda_arg", [True, False])
@pytest.mark.parametrize("mmoda_path", [True, False])
def test_posix_download_file_mmoda_url(client, public, mmoda_arg, mmoda_path):
    if not mmoda_path:
        fits_file_url = 'https://www.astro.unige.ch/test.fits'
    else:
        fits_file_url = 'https://www.astro.unige.ch/mmoda/dispatch-data/test.fits'
    query_string = {}
    url_params = {}
    if not public:
        query_string['_token'] = 'test_token'
        url_params['token'] = 'test_token'
    if mmoda_arg:
        url_params['_is_mmoda_url'] = 'True'
    url_params_dict = urlencode(url_params)
    fits_file_url = f'{fits_file_url}?{url_params_dict}'
    query_string['fits_file_path'] = fits_file_url
    r = client.get('/api/v1.0/get/testposixpath', query_string=query_string)
    assert r.json['exceptions'][0] == ("Exception('An issue, due to invalid status code, occurred when attempting to getting the file size at the url "
                                       f"{fits_file_url}. This might be related to an "
                                       "invalid url, please check the input provided')")

@pytest.mark.parametrize("public", [True, False])
@pytest.mark.parametrize("host", ["localhost", "0.0.0.0"])
def test_posix_download_file_local_mmoda_url(client, public, host):
    fits_file_url = f'https://{host}:1234/test.fits'
    query_string = {}
    url_params = {'_is_mmoda_url': True}
    if not public:
        query_string['_token'] = 'test_token'
        url_params['token'] = 'test_token'
    url_params_dict = urlencode(url_params)
    fits_file_url = f'{fits_file_url}?{url_params_dict}'
    query_string['fits_file_path'] = fits_file_url
    r = client.get('/api/v1.0/get/testposixpath', query_string=query_string)
    assert r.json['exceptions'][0] == ("Exception('An issue, due to connection error, occurred when attempting to getting the file size at the url "
                                       f"{fits_file_url}. This might be related to an "
                                       "invalid url, please check the input provided')")

def test_boolean_default(client):
    r = client.get('/api/v1.0/get/testbool')
    assert r.json['output']['output'] == 'boolean True'
    
def test_boolean_false(client):
    r = client.get('/api/v1.0/get/testbool', query_string={'boolpar': 'false'})
    assert r.json['output']['output'] == 'boolean False'
    

def test_boolean_False(client):
    r = client.get('/api/v1.0/get/testbool', query_string={'boolpar': 'False'})
    assert r.json['output']['output'] == 'boolean False'
    
def test_boolean_empty(client):
    r = client.get('/api/v1.0/get/testbool', query_string={'boolpar': ''})
    assert r.json['output']['output'] == 'boolean False'
    
def test_boolean_true(client):
    r = client.get('/api/v1.0/get/testbool', query_string={'boolpar': 'true'})
    assert r.json['output']['output'] == 'boolean True'
    
def test_boolean_wrong(client):
    r = client.get('/api/v1.0/get/testbool', query_string={'boolpar': 'spam'})
    assert r.json['issues'][0] == 'Parameter boolpar value "spam" can not be interpreted as boolean.'
    
def test_list(client):
    r = client.get('/api/v1.0/get/structured_input', query_string={'lst': '[1, 2, 3]'})
    assert r.json['output']['lst'] == [1, 2, 3]

def test_list_wrong(client):
    r = client.get('/api/v1.0/get/structured_input', query_string={'lst': 'baz'})
    assert r.json['issues'][0] == 'Parameter lst value "baz" can not be interpreted as list.'
    
def test_dict(client):
    r = client.get('/api/v1.0/get/structured_input', query_string={'dct': '{"foo": "bar"}'})
    assert r.json['output']['dct'] == {"foo": "bar"}

def test_dict_complex(client):
    r = client.get('/api/v1.0/get/structured_input', 
                   query_string={'dct': '{"foo": ["bar", "baz"], "spam": ["ham", "eggs"]}'})
    assert r.json['output']['dct'] == {"foo": ["bar", "baz"], "spam": ["ham", "eggs"]}

def test_dict_wrong(client):
    r = client.get('/api/v1.0/get/structured_input', query_string={'dct': 'baz'})
    assert r.json['issues'][0] == 'Parameter dct value "baz" can not be interpreted as dict.'


@pytest.mark.parametrize('inp,outp', [({'opt': None}, {'opt': None}),
                                      ({'opt': 10}, {'opt': 10.}),
                                      ({'intfloat': 25}, {'intfloat': 25.}),
                                      ({'intfloat': 25.}, {'intfloat': 25.}),
                                      ({'inten': 20}, {'inten': 20}),
                                      ({'flag': False}, {'flag': False}),
                                      ({'flag': 0}, {'flag': False}),
                                      ({'string_param': 'contains = symbol'}, {'string_param': 'contains = symbol'}),
                                      ({'otheropt': '\x00'}, {'otheropt': None}),
                                      ({'short_string_param': ''}, {'short_string_param': ''}),
                                      ])
def test_type_casting(client, inp, outp):
    r = client.get('/api/v1.0/options')
    pars = r.json['multiline']['parameters']
    defaults = {k: v['default_value'] for k, v in pars.items()}

    r = client.get('/api/v1.0/get/multiline', query_string=inp)
    assert r.json['output']['echo'] == {**defaults, **outp}
    for k, v in outp.items():
        assert isinstance(r.json['output']['echo'][k], type(v))

@pytest.mark.parametrize('query', [{'inten': 20.1}, {'intfloat': '\x00'}])
def test_casting_invalid(client, query):
    r = client.get('/api/v1.0/get/multiline', query_string=query)
    assert len(r.json['issues']) > 0

    # async
    query.update({'_async_request': 'yes'})
    r=client.get('/api/v1.0/get/multiline', 
                 query_string=query)
    assert len(r.json['data']['exceptions']) > 0

