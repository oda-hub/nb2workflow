import pytest
import nb2workflow
import os

@pytest.fixture
def app():
    testfiles_path = os.path.join(os.path.dirname(__file__), 'testfiles')
    app = nb2workflow.service.app
    app.notebook_adapters = nb2workflow.nbadapter.find_notebooks(testfiles_path)
    nb2workflow.service.setup_routes(app)
    print("creating app")
    return app


def test_posix_download_file(client):
    r = client.get('/api/v1.0/get/testposixpath')
    assert r.json['output']['output_file_download'] == 'file not downloaded'

def test_posix_download_file_with_arg(client):
    r = client.get('/api/v1.0/get/testposixpath', query_string={'fits_file_path': 'https://fits.gsfc.nasa.gov/samples/testkeys.fits'})
    assert r.json['output']['output_file_download'] == 'file downloaded successfully'

def test_posix_download_file_with_arg_wrong_url(client):
    r = client.get('/api/v1.0/get/testposixpath', query_string={'fits_file_path': 'https://fits.gsfc.nasa.gov/samples/aaaaaa.fits'})
    assert r.json['exceptions'][0] == ("Exception('An issue occurred when attempting to download the url "
                                       "https://fits.gsfc.nasa.gov/samples/aaaaaa.fits, this might be related "
                                       "to an invalid url, please check the input provided')")

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
