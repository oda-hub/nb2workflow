import rdflib
import logging

oda_ontology_prefix = "http://odahub.io/ontology#"
oda = rdflib.Namespace(oda_ontology_prefix)
oda_ontology_integral_prefix = "http://odahub.io/ontology/integral#"
oda_integral = rdflib.Namespace(oda_ontology_integral_prefix)
oda_ontology_preview_prefix = "http://odahub.io/ontology/preview/"
oda_preview = rdflib.Namespace(oda_ontology_preview_prefix)

rdfs = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
rdf = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
rdf_xmlschema = rdflib.Namespace("http://www.w3.org/2001/XMLSchema#")

wfl_p_ns_str = 'http://odahub.io/workflows/{workflow_name}/parameter_bindings#'
wfl_o_ns_str = 'http://odahub.io/workflows/{workflow_name}/output_bindings#'

subClassOf = rdfs['subClassOf']
rdf_type = rdf['type']


FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger("nb2workflow")
logger.setLevel(level=logging.DEBUG)

def test_nbadapter_repo_annotations(test_notebook_repo):
    from nb2workflow.nbadapter import NotebookAdapter, find_notebooks
    from nb2workflow import ontology

    nbas = find_notebooks(test_notebook_repo)

    
    G = rdflib.Graph()
    G.bind("oda", oda)
    G.parse(data=ontology.service_semantic_signature(nbas), format="xml")

    logger.info(G.serialize(format="turtle"))
    
    # note that here, oda:keV is understood as "some parameter that is expressed in keV"
    # this is different from unit:keV which is a subClass of Unit.

    assert (oda["emin_keV"], subClassOf, oda["emin"]) in G
    assert (oda["emin_keV"], subClassOf, oda["keV"]) in G

    assert (oda["1000integer_15integer_emax_keV_lower_limit_upper_limit"], subClassOf, oda["keV"]) in G
    assert (oda["1000integer_15integer_emax_keV_lower_limit_upper_limit"], oda["lower_limit"], rdflib.Literal(15)) in G

    
def test_nb2rdf(test_notebook_repo):
    from nb2workflow.nbadapter import NotebookAdapter, find_notebooks
    from nb2workflow import ontology

    nbas = find_notebooks(test_notebook_repo)

    G = rdflib.Graph()

    for target, nba in nbas.items():
        rdf_nb = ontology.nb2rdf(nba.notebook_fn)
        G.parse(data=rdf_nb)
        G.bind("oda", oda)
        G.bind('rdfs', rdfs)
        wfl_p_ns = rdflib.Namespace(wfl_p_ns_str.format(workflow_name=nba.unique_name))
        assert (wfl_p_ns["scwid"], oda["value"], rdflib.Literal("066500110010.001")) in G
        assert (wfl_p_ns["scwid"], rdf_type, oda_integral["ScWID"]) in G
        assert (wfl_p_ns["nbins"], oda["value"], rdflib.Literal(100)) in G
        assert (wfl_p_ns["nbins"], rdf_type, oda["Integer"]) in G
        assert (wfl_p_ns["sleep"], oda["value"], rdflib.Literal(0)) in G
        assert (wfl_p_ns["sleep"], rdf_type, oda["Integer"]) in G

        wfl_o_ns = rdflib.Namespace(wfl_o_ns_str.format(workflow_name=nba.unique_name))
        assert (wfl_o_ns["spectrum_png"], oda["value"], rdflib.Literal("fn")) in G
        assert (wfl_o_ns["spectrum_png"], rdf_type, oda_preview["png"]) in G
        assert (wfl_o_ns["spectrum"], oda["value"], rdflib.Literal("h[0].tolist()")) in G
        assert (wfl_o_ns["spectrum"], rdf_type, oda_preview["png"]) in G

