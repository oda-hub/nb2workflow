import json
import os
from oda_api.data_products import ODAAstropyTable
from nb2workflow.json import CustomJSONEncoder
import numpy as np
from astropy.table import Table
from io import StringIO
import pytest
import nb2workflow

@pytest.fixture
def app():
    testfiles_path = os.path.join(os.path.dirname(__file__), 'testfiles')
    app = nb2workflow.service.app
    app.notebook_adapters = nb2workflow.nbadapter.find_notebooks(testfiles_path)
    nb2workflow.service.setup_routes(app)
    print("creating app")
    return app

def test_oda_astropy_table_encoding():
    data = np.zeros((10, 2))
    data[:,0] = range(len(data))
    data[:,1] = range(len(data), 0, -1)
    atable = Table(data, names=['a', 'b'])
    tabp = ODAAstropyTable(atable)
    
    with StringIO() as fd:
        atable.write(fd, format='ascii.ecsv')
        ascii_repr = fd.getvalue()
    
    encoded_table = CustomJSONEncoder().encode(tabp)
    assert json.loads(encoded_table)['ascii'] == ascii_repr

def test_oda_lightcurve_service(client):
    r = client.get('/api/v1.0/get/lightcurve')
    enc_lc = json.loads(r.json['output']['result'])
    assert enc_lc['data_unit_list'][1]['dt'] == "(numpy.record, [('TIME', '<f8'), ('MAG', '<f8'), ('ERROR', '<f8')])"