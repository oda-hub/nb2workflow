import pytest
from nb2workflow.nbadapter import reconcile_python_type
from typeguard import TypeCheckError

ttl_prefix = """
@prefix oda: <http://odahub.io/ontology#> . 
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
@prefix owl: <http://www.w3.org/2002/07/owl#> . 
        
"""

@pytest.mark.parametrize('value,type_annotation,owl_type,extra_ttl,expected_type,expected_optional',
                         [('foo', None, None, '', str, False),
                          (1, None, None, '', int, False),
                          (1.2, None, None, '', float, False),
                          (1.2, None, 'oda:Float', '', float, False),
                          (True, None, 'oda:Boolean', '', bool, False),
                          ('2017-03-06T13:26:48.0', None, 'oda:StartTimeISOT', '', str, False),
                          (60446.5, None, 'oda:StartTimeMJD', '', float, False),
                          (60446, None, 'oda:StartTimeMJD', '', float, False),
                          (25.5, None, 'oda:OptFloat', 'oda:OptFloat rdfs:subClassOf oda:Float, oda:optional .', float, True),
                          (None, None, 'oda:OptFloat', 'oda:OptFloat rdfs:subClassOf oda:Float, oda:optional .', float, True),

                          (1.2, 'float', None, '', float, False),
                          ('2017-03-06T13:26:48.0', 'str', None, '', str, False),
                          (60446.5, 'float', None, '', float, False),
                          (60446, 'float', None, '', float, False),
                          (True, 'bool', None, '', bool, False),
                          (25.5, 'float | None', None, '', float, True),
                          (25.5, 'Optional[float]', None, '', float, True),
                          (25.5, 'Union[float, None]', None, '', float, True),
                          ({"foo": ["bar", "baz"]}, 'dict[str,list]', None, '', dict, False),

                          (None, 'str | None', None, '', str, True),
                          (None, 'bool|None', None, '', bool, True),
                          (None, 'Optional[list[int]]', None, '', list, True),
                          (None, 'Optional[List[str]]', None, '', list, True),

                          (None, 'float | None', 'oda:Float', '', float, True),
                          (None, 'float', 'oda:OptFloat', 'oda:OptFloat rdfs:subClassOf oda:Float, oda:optional .', float, True),
                          (None, 'float | None', 'oda:OptFloat', 'oda:OptFloat rdfs:subClassOf oda:Float, oda:optional .', float, True),

                          (60446, 'int', 'oda:StartTimeMJD', '', int, False),
                          (60446, 'float', 'oda:Integer', '', int, False),

                          ('foo', None, 'oda:WithNoTypeDefined', '', str, False),
                          ({'foo': ['bar', 'baz']}, None, 'oda:WithNoTypeDefined', '', dict, False),
                          ]
                         )
def test_reconcile_python_type(value,type_annotation,owl_type,extra_ttl,expected_type,expected_optional):
    assert reconcile_python_type(value=value, 
                                 type_annotation=type_annotation,
                                 owl_type=owl_type,
                                 extra_ttl=ttl_prefix+extra_ttl) == (expected_type, expected_optional)
    
@pytest.mark.parametrize('value,type_annotation,owl_type',
                         [(None, None, None),
                          (None, None, 'oda:Float'),
                          (None, 'float', None),
                          (None, 'str', None),
                          (None, 'float', 'oda:Float'),
                          (None, 'float | None', 'oda:String'),
                          
                          (False, 'int | None', None),
                          (1, 'bool', None),

                          ('foo', None, 'oda:Float'),
                          (5.0, None, 'oda:Integer'),
                          (5.0, None, 'oda:String'),

                          ('foo', 'float', None),
                          (5.0, 'int', None),
                          (5.0, 'str', None),
                          
                          (5.0, 'str', 'oda:Float'),

                          (None, None, 'oda:optional'), # optional have undefined type
                          ]
                         )
def test_reconcile_python_type_failing(value,type_annotation,owl_type):
    with pytest.raises(TypeCheckError):
        reconcile_python_type(value=value, 
                              type_annotation=type_annotation,
                              owl_type=owl_type)


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