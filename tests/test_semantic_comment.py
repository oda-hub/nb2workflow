import pytest

import rdflib
import rdflib.compare

from nb2workflow.semantics import understand_comment_references
from nb2workflow.nbadapter import parse_nbline


def normalize(x):
    G = rdflib.Graph()
    G.parse(data=x, format='ttl')
    return list(sorted(rdflib.compare.to_canonical_graph(G)))


def test_semantic_comments():

    nb_uri = rdflib.URIRef("http://mynb")

    r = understand_comment_references("oda:CRBeamS3 a oda:S3 .", base_uri=nb_uri)
    assert r['owl_type'] == str(nb_uri)

    r = understand_comment_references('oda:CRBeamS3 oda:resourceBindingEnvVarName "CRBEAM_S3_CREDENTIALS" .', base_uri=nb_uri)
    assert r['owl_type'] == str(nb_uri)

    r = understand_comment_references("http://odahub.io/ontology#StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"

    # allow some extra comment
    r = understand_comment_references("http://odahub.io/ontology#StartTimeISOT . # and some text")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"    

    r = understand_comment_references("oda:StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"




    # note, that as of now - the comment has to be a complete type annotation, default value type is not used along with addition annotations
    # TODO: make a reasonable reconciliation-combination of the types
    r = understand_comment_references("oda:upper_limit 1") 
    assert r['owl_type'] == None
    assert r['extra_ttl'] == None

    r = understand_comment_references("oda:upper_limit 1", fallback_type='http://www.w3.org/2001/XMLSchema#int') 
    assert r['owl_type'] is not None
    assert r['extra_ttl'] is not None



    r = understand_comment_references("oda:energyMin; oda:unit unit:keV")
    assert r['owl_type'] == "http://odahub.io/ontology#energyMin_keV_unit"
    assert normalize(r['extra_ttl']) == normalize("""
            @prefix oda: <http://odahub.io/ontology#> . 
            @prefix unit: <http://odahub.io/ontology/unit#> . 
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
            
            oda:energyMin_keV_unit rdfs:subClassOf oda:energyMin;
                              oda:unit unit:keV .
            """)
 
    r = understand_comment_references("oda:energyMin, oda:keV")
    assert r['owl_type'] == "http://odahub.io/ontology#energyMin_keV"
    assert normalize(r['extra_ttl']) == normalize("""
            @prefix oda: <http://odahub.io/ontology#> . 
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
            
            oda:energyMin_keV rdfs:subClassOf oda:energyMin, oda:keV .
            """)
    
    r = understand_comment_references("oda:energyMin; oda:lower_limit 3;  oda:upper_limit 30")
    assert r['owl_type'] == "http://odahub.io/ontology#30integer_3integer_energyMin_lower_limit_upper_limit"
    assert normalize(r['extra_ttl']) == normalize("""@prefix oda: <http://odahub.io/ontology#> . 
            @prefix owl: <http://www.w3.org/2002/07/owl#> . 
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
            
            oda:30integer_3integer_energyMin_lower_limit_upper_limit rdfs:subClassOf oda:energyMin; 
                  oda:lower_limit 3 ; 
                  oda:upper_limit 30 .
            """)

    r = understand_comment_references("oda:energyMin; oda:limits 3, 30")
    assert r['owl_type'] == "http://odahub.io/ontology#30integer_3integer_energyMin_lower_limit_upper_limit"
    assert normalize(r['extra_ttl']) == normalize("""
            @prefix oda: <http://odahub.io/ontology#> . 
            @prefix owl: <http://www.w3.org/2002/07/owl#> . 
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
            
            oda:30integer_3integer_energyMin_lower_limit_upper_limit rdfs:subClassOf 
                  oda:energyMin; 
                  oda:lower_limit 3 ; 
                  oda:upper_limit 30 .
            """)
    
    r = parse_nbline('# oda:version "v1"', nb_uri)
    assert r['owl_type'] == str(nb_uri)
    assert normalize(r['extra_ttl']) == normalize('@prefix oda: <http://odahub.io/ontology#> . <http://mynb> oda:version "v1" .')

    r = parse_nbline('# oda:reference https://doi.org/10.1051/0004-6361/202037850', nb_uri)        
    assert r['owl_type'] == str(nb_uri)
    assert normalize(r['extra_ttl']) == normalize('@prefix oda: <http://odahub.io/ontology#> . <http://mynb> oda:reference <https://doi.org/10.1051/0004-6361/202037850> .')

    r = parse_nbline('# oda:relevantForObject oda:Crab', nb_uri)
    assert r['owl_type'] == str(nb_uri)
    assert normalize(r['extra_ttl']) == normalize('@prefix oda: <http://odahub.io/ontology#> . <http://mynb> oda:relevantForObject oda:Crab .')
    

@pytest.mark.parametrize("comment, expected_owl_type, expected_value", [
    ("http://odahub.io/ontology#TimeIntervalSeconds ; oda:upper_limit 20.", "200decimal_TimeIntervalSeconds_upper_limit", "20.0"),
    ("http://odahub.io/ontology#TimeIntervalSeconds ; oda:upper_limit 20 .", "20integer_TimeIntervalSeconds_upper_limit", "20"),
    ("http://odahub.io/ontology#TimeIntervalSeconds ; oda:upper_limit 20", "20integer_TimeIntervalSeconds_upper_limit", "20"),
    ("http://odahub.io/ontology#TimeIntervalSeconds ; oda:upper_limit 20.0", "200decimal_TimeIntervalSeconds_upper_limit", "20.0"),
])
def test_single_ul(comment, expected_owl_type, expected_value):
    r = understand_comment_references(comment)
    assert r['owl_type'] == "http://odahub.io/ontology#" + expected_owl_type
    assert normalize(r['extra_ttl']) == normalize(
       f'''@prefix oda: <http://odahub.io/ontology#> .
           @prefix owl: <http://www.w3.org/2002/07/owl#> . 
           @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
           @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
            
           oda:{expected_owl_type} rdfs:subClassOf oda:TimeIntervalSeconds;
                                   oda:upper_limit {expected_value} .
            ''')

    
def test_semantic_nbline():
    from nb2workflow.nbadapter import parse_nbline

    r = parse_nbline("t1=1 # http://odahub.io/ontology#StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"
    assert normalize(r['extra_ttl']) == []
    
    r = parse_nbline("t2=2. # http://odahub.io/ontology#StartTimeISOT . # and some text")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"    

    r = parse_nbline("t3 # oda:StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"

    r = parse_nbline("tstart_seconds=1 # oda:upper_limit 2") 
    assert r['owl_type'] == "http://odahub.io/ontology#2integer_Integer_upper_limit"
    assert normalize(r['extra_ttl']) == normalize("""
        @prefix oda: <http://odahub.io/ontology#> . 
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
        @prefix owl: <http://www.w3.org/2002/07/owl#> . 
        
        oda:2integer_Integer_upper_limit rdfs:subClassOf oda:Integer;
                                     oda:upper_limit 2 .
    """)

    r = parse_nbline("result=obj_results # http://odahub.io/ontology#LightCurveList") 
    assert r['owl_type'] == "http://odahub.io/ontology#LightCurveList"
    assert normalize(r['extra_ttl']) == []
