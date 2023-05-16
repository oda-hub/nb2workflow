import rdflib
import logging

oda_ontology_prefix = "http://odahub.io/ontology#"    
oda = rdflib.Namespace(oda_ontology_prefix)
rdfs = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")

subClassOf = rdfs['subClassOf']


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
