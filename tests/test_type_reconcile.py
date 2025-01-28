import pytest
from nb2workflow.nbadapter import reconcile_python_type
from typeguard import TypeCheckError

ttl_prefix = """
@prefix oda: <http://odahub.io/ontology#> . 
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
@prefix owl: <http://www.w3.org/2002/07/owl#> . 
        
"""

@pytest.mark.parametrize(
        'value,type_annotation,owl_type,extra_ttl,expected_type,expected_optional,expected_is_file',
        [
        ('foo', None, None, '', str, False, False),
        (1, None, None, '', int, False, False),
        (1.2, None, None, '', float, False, False),
        (1.2, None, 'oda:Float', '', float, False, False),
        (True, None, 'oda:Boolean', '', bool, False, False),
        ('2017-03-06T13:26:48.0', None, 'oda:StartTimeISOT', '', str, False, False),
        (60446.5, None, 'oda:StartTimeMJD', '', float, False, False),
        (60446, None, 'oda:StartTimeMJD', '', float, False, False),
        (25.5, None, 'oda:OptFloat', 'oda:OptFloat rdfs:subClassOf oda:Float, oda:optional .', float, True, False),
        (None, None, 'oda:OptFloat', 'oda:OptFloat rdfs:subClassOf oda:Float, oda:optional .', float, True, False),

        (1.2, 'float', None, '', float, False, False),
        ('2017-03-06T13:26:48.0', 'str', None, '', str, False, False),
        (60446.5, 'float', None, '', float, False, False),
        (60446, 'float', None, '', float, False, False),
        (True, 'bool', None, '', bool, False, False),
        (25.5, 'float | None', None, '', float, True, False),
        (25.5, 'Optional[float]', None, '', float, True, False),
        (25.5, 'Union[float, None]', None, '', float, True, False),
        ({"foo": ["bar", "baz"]}, 'dict[str,list]', None, '', dict, False, False),

        (None, 'str | None', None, '', str, True, False),
        (None, 'bool|None', None, '', bool, True, False),
        (None, 'Optional[list[int]]', None, '', list, True, False),
        (None, 'Optional[List[str]]', None, '', list, True, False),

        (None, 'float | None', 'oda:Float', '', float, True, False),
        (None, 'float', 'oda:OptFloat', 'oda:OptFloat rdfs:subClassOf oda:Float, oda:optional .', float, True, False),
        (None, 'float | None', 'oda:OptFloat', 'oda:OptFloat rdfs:subClassOf oda:Float, oda:optional .', float, True, False),

        (60446, 'int', 'oda:StartTimeMJD', '', int, False, False),
        (60446, 'float', 'oda:Integer', '', int, False, False),

        ('foo', None, 'oda:WithNoTypeDefined', '', str, False, False),
        ({'foo': ['bar', 'baz']}, None, 'oda:WithNoTypeDefined', '', dict, False, False),

        ('file-name.fits', None, 'oda:POSIXPath', '', str, False, True),
        ('file-name.fits', None, 'oda:OptPPath', 'oda:OptPPath rdfs:subClassOf oda:POSIXPath, oda:optional .', str, True, True),
        (None, None, 'oda:OptPPath', 'oda:OptPPath rdfs:subClassOf oda:POSIXPath, oda:optional .', str, True, True),
        ('file-name.fits', 'str | None', 'oda:OptPPath', 'oda:OptPPath rdfs:subClassOf oda:POSIXPath, oda:optional .', str, True, True),
        ('file-name.fits', 'str | None', 'oda:POSIXPath', '', str, True, True),
        (None, 'str | None', 'oda:POSIXPath', '', str, True, True),
        ])
def test_reconcile_python_type(value,type_annotation,owl_type,extra_ttl,expected_type,expected_optional,expected_is_file):
    assert reconcile_python_type(value=value, 
                                 type_annotation=type_annotation,
                                 owl_type=owl_type,
                                 extra_ttl=ttl_prefix+extra_ttl) == (expected_type, expected_optional,expected_is_file)
    
@pytest.mark.parametrize('value,type_annotation,owl_type,extra_ttl',
                         [(None, None, None, ''),
                          (None, None, 'oda:Float', ''),
                          (None, 'float', None, ''),
                          (None, 'str', None, ''),
                          (None, 'float', 'oda:Float', ''),
                          (None, 'float | None', 'oda:String', ''),
                          
                          (False, 'int | None', None, ''),
                          (1, 'bool', None, ''),

                          ('foo', None, 'oda:Float', ''),
                          (5.0, None, 'oda:Integer', ''),
                          (5.0, None, 'oda:String', ''),

                          ('foo', 'float', None, ''),
                          (5.0, 'int', None, ''),
                          (5.0, 'str', None, ''),
                          
                          (5.0, 'str', 'oda:Float', ''),

                          (None, None, 'oda:optional', ''), # optional have undefined type

                          (None, None, 'oda:POSIXPath', ''),
                          ('', None, 'oda:POSIXPath', ''),
                          ('', 'str | None', 'oda:POSIXPath', ''),
                          ('', None, 'oda:OptPPath', 'oda:OptPPath rdfs:subClassOf oda:POSIXPath, oda:optional .'),
                          ]
                         )
def test_reconcile_python_type_failing(value,type_annotation,owl_type,extra_ttl):
    with pytest.raises(TypeCheckError):
        reconcile_python_type(value=value, 
                              type_annotation=type_annotation,
                              owl_type=owl_type,
                              extra_ttl=ttl_prefix+extra_ttl)


@pytest.mark.parametrize('value,expected_type', [(False, bool),
                                                 (1, int),
                                                 (1.1, float),
                                                 ('foo', str),
                                                 ({'foo': 'bar'}, dict),
                                                 ([1, 2, 3], list),
                                                 ])
def test_unknown_owl(value,expected_type,caplog):
    extype = reconcile_python_type(value=value,
                                   owl_type='http://odahub.io/ontology#UnknownType')
    assert extype[0] == expected_type
    assert 'Unknown datatype for owl_uri' in caplog.text