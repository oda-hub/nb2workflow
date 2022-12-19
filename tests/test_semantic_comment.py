

import re


def test_semantic_comments():
    from nb2workflow.ontology import understand_comment_references

    r = understand_comment_references("http://odahub.io/ontology#StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"

    # allow some extra comment
    r = understand_comment_references("http://odahub.io/ontology#StartTimeISOT and some text")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"    

    r = understand_comment_references("oda:StartTimeISOT")
    assert r['owl_type'] == "http://odahub.io/ontology#StartTimeISOT"


    normalize = lambda x: re.sub(r"[ \n]+", " ", x).strip()

    r = understand_comment_references("oda:energyMin, oda:keV")
    assert r['owl_type'] == "http://odahub.io/ontology#energyMinkeV"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . oda:energyMinkeV a oda:energyMin, oda:keV ."
    

    r = understand_comment_references("oda:energyMin; oda:lower_bound 3;  oda:upper_bound 30")
    assert r['owl_type'] == "http://odahub.io/ontology#energyMin"
    assert normalize(r['extra_ttl']) == "@prefix oda: <http://odahub.io/ontology#> . oda:energyMin; oda:lower_bound 3; oda:upper_bound 30 ."

    r = understand_comment_references("oda:energyMin; oda:lower_bound 3; oda:upper_bound 30")

    r = understand_comment_references("oda:energyMin; oda:min_default_max [3, 10, 30]")


    # r = understand_comment_references("oda:energyMin; oda:min_default_max [3, 10, 30]")
