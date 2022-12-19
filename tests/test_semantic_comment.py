

import re


def test_semantic_comments():
    from nb2workflow.semantics import understand_comment_references

    r = understand_comment_references("http://odahub.io/ontology#StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"

    # allow some extra comment
    r = understand_comment_references("http://odahub.io/ontology#StartTimeISOT . # and some text")
    # assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"    

    r = understand_comment_references("oda:StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"


    normalize = lambda x: re.sub(r"[ \n]+", " ", x).strip()

    r = understand_comment_references("oda:energyMin, oda:keV")
    assert r['owl_type'] == "http://odahub.io/ontology#energyMin_keV"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . oda:energyMin_keV a oda:energyMin, oda:keV ."
    

    r = understand_comment_references("oda:energyMin; oda:lower_bound 3;  oda:upper_bound 30")
    assert r['owl_type'] == "http://odahub.io/ontology#lower_bound_3integer_upper_bound_30integer_energyMin"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:lower_bound_3integer_upper_bound_30integer_energyMin a oda:energyMin ; oda:lower_bound 3 ; oda:upper_bound 30 ."

    # TODO: make inference to merge the two
    r = understand_comment_references("oda:energyMin; oda:limits 3, 30")
    assert r['owl_type'] == "http://odahub.io/ontology#limits_3integer_limits_30integer_energyMin"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:limits_3integer_limits_30integer_energyMin a oda:energyMin ; oda:limits 3, 30 ."


    r = understand_comment_references("oda:limits 3, 30")
    assert r['owl_type'] == "http://odahub.io/ontology#limits_3integer_limits_30integer"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . oda:limits_3integer_limits_30integer oda:limits 3, 30 ."

