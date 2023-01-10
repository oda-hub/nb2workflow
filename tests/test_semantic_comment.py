

import re


def test_semantic_comments():
    from nb2workflow.semantics import understand_comment_references

    r = understand_comment_references("https://odahub.io/ontology#StartTimeISOT")
    assert r['owl_type'] == "https://odahub.io/ontology#StartTimeISOT"

    # allow some extra comment
    r = understand_comment_references("https://odahub.io/ontology#StartTimeISOT . # and some text")
    assert r['owl_type'] == "https://odahub.io/ontology#StartTimeISOT"    

    r = understand_comment_references("oda:StartTimeISOT")
    assert r['owl_type'] == "https://odahub.io/ontology#StartTimeISOT"


    normalize = lambda x: re.sub(r"[ \n]+", " ", x).strip()

    r = understand_comment_references("oda:energyMin, oda:keV")
    assert r['owl_type'] == "https://odahub.io/ontology#energyMin_keV"
    assert normalize(r['extra_ttl']) == "@prefix oda: <https://odahub.io/ontology#> . oda:energyMin_keV a oda:energyMin, oda:keV ."
    
    r = understand_comment_references("oda:energyMin; oda:lower_limit 3;  oda:upper_limit 30")
    assert r['owl_type'] == "https://odahub.io/ontology#energyMin_lower_limit_3integer_upper_limit_30integer"
    assert normalize(r['extra_ttl']) == "@prefix oda: <https://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:energyMin_lower_limit_3integer_upper_limit_30integer a oda:energyMin ; oda:lower_limit 3 ; oda:upper_limit 30 ."

    r = understand_comment_references("oda:energyMin; oda:limits 3, 30")
    assert r['owl_type'] == "https://odahub.io/ontology#energyMin_lower_limit_3integer_upper_limit_30integer"
    assert normalize(r['extra_ttl']) == "@prefix oda: <https://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:energyMin_lower_limit_3integer_upper_limit_30integer a oda:energyMin ; oda:lower_limit 3 ; oda:upper_limit 30 ."

    r = understand_comment_references("oda:limits 3, 30")
    assert r['owl_type'] == "https://odahub.io/ontology#lower_limit_3integer_upper_limit_30integer"
    assert normalize(r['extra_ttl']) == "@prefix oda: <https://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:lower_limit_3integer_upper_limit_30integer oda:lower_limit 3 ; oda:upper_limit 30 ."


    # TODO test free-line comments