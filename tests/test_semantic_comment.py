import re
import rdflib

normalize = lambda x: re.sub(r"[ \n]+", " ", x).strip()


def test_semantic_comments():
    from nb2workflow.semantics import understand_comment_references
    from nb2workflow.nbadapter import parse_nbline

    nb_uri = rdflib.URIRef("http://mynb")    

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

    r = understand_comment_references("oda:Integer; oda:upper_limit 1")
    assert normalize(r['owl_type']) == 'http://odahub.io/ontology#1integer_Integer_upper_limit'
    assert normalize(r['extra_ttl']) == '@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:1integer_Integer_upper_limit a oda:Integer ; oda:upper_limit 1 .'

    r = understand_comment_references("oda:energyMin, oda:keV")
    assert r['owl_type'] == "http://odahub.io/ontology#energyMin_keV"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . oda:energyMin_keV a oda:energyMin, oda:keV ."
    
    r = understand_comment_references("oda:energyMin; oda:lower_limit 3;  oda:upper_limit 30")
    assert r['owl_type'] == "http://odahub.io/ontology#30integer_3integer_energyMin_lower_limit_upper_limit"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:30integer_3integer_energyMin_lower_limit_upper_limit a oda:energyMin ; oda:lower_limit 3 ; oda:upper_limit 30 ."

    r = understand_comment_references("oda:energyMin; oda:limits 3, 30")
    assert r['owl_type'] == "http://odahub.io/ontology#30integer_3integer_energyMin_lower_limit_upper_limit"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:30integer_3integer_energyMin_lower_limit_upper_limit a oda:energyMin ; oda:lower_limit 3 ; oda:upper_limit 30 ."
    
    r = parse_nbline('# oda:version "v1"', nb_uri)
    assert r['owl_type'] == str(nb_uri)
    assert normalize(r['extra_ttl']) == '@prefix oda: <http://odahub.io/ontology#> . <http://mynb> oda:version "v1" .'


def test_semantic_nbline():
    from nb2workflow.nbadapter import parse_nbline

    nb_uri = rdflib.URIRef("http://mynb")    

    r = parse_nbline("t1=1 # http://odahub.io/ontology#StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"
    assert normalize(r['extra_ttl']) == ""
    
    r = parse_nbline("t2=2. # http://odahub.io/ontology#StartTimeISOT . # and some text")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"    

    r = parse_nbline("t3 # oda:StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"

    r = parse_nbline("tstart_seconds=1 # oda:upper_limit 2") 
    assert r['owl_type'] == "http://odahub.io/ontology#2integer_int_upper_limit"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:2integer_int_upper_limit a xsd:int ; oda:upper_limit 2 ."

    r = parse_nbline("result=obj_results # http://odahub.io/ontology#LightCurveList") 
    assert r['owl_type'] == "http://odahub.io/ontology#LightCurveList"
    assert normalize(r['extra_ttl']) == ""
