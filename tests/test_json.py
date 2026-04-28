import json

from nb2workflow.json import CustomJSONEncoder
import numpy as np
from io import StringIO


def test_oda_astropy_table_encoding():
    from oda_api.data_products import ODAAstropyTable
    from astropy.table import Table
    
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