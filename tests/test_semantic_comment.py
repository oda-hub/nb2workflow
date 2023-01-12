import re
import rdflib


def test_semantic_comments():
    from nb2workflow.semantics import understand_comment_references
    from nb2workflow.nbadapter import parse_nbline

    r = understand_comment_references("http://odahub.io/ontology#StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"

    # allow some extra comment
    r = understand_comment_references("http://odahub.io/ontology#StartTimeISOT . # and some text")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"    

    r = understand_comment_references("oda:StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"


    normalize = lambda x: re.sub(r"[ \n]+", " ", x).strip()

    r = understand_comment_references("oda:energyMin, oda:keV")
    assert r['owl_type'] == "http://odahub.io/ontology#energyMin_keV"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . oda:energyMin_keV a oda:energyMin, oda:keV ."
    
    r = understand_comment_references("oda:energyMin; oda:lower_limit 3;  oda:upper_limit 30")
    assert r['owl_type'] == "http://odahub.io/ontology#30integer_3integer_energyMin_lower_limit_upper_limit"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:30integer_3integer_energyMin_lower_limit_upper_limit a oda:energyMin ; oda:lower_limit 3 ; oda:upper_limit 30 ."

    r = understand_comment_references("oda:energyMin; oda:limits 3, 30")
    assert r['owl_type'] == "http://odahub.io/ontology#30integer_3integer_energyMin_lower_limit_upper_limit"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:30integer_3integer_energyMin_lower_limit_upper_limit a oda:energyMin ; oda:lower_limit 3 ; oda:upper_limit 30 ."

    r = understand_comment_references("oda:limits 3, 30")
    assert r['owl_type'] == "http://odahub.io/ontology#30integer_3integer_lower_limit_upper_limit"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:30integer_3integer_lower_limit_upper_limit oda:lower_limit 3 ; oda:upper_limit 30 ."

    # comments
    nb_uri = rdflib.URIRef("http://mynb")
    r = parse_nbline('# oda:version "v1"', nb_uri)
    assert r['owl_type'] == str(nb_uri)
    assert normalize(r['extra_ttl']) == '@prefix oda: <http://odahub.io/ontology#> . <http://mynb> oda:version "v1" .'