import os
import rdflib
import logging
import pytest

a = rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
oda_ontology_prefix = "https://odahub.io/ontology#"    
oda = rdflib.Namespace(oda_ontology_prefix)

# test_notebook_repo=os.environ.get('TEST_NOTEBOOK_REPO')

#logger=logging.getLogger("nb2workflow")

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
    
    assert (oda["emin_keV"], a, oda["emin"]) in G
    assert (oda["emin_keV"], a, oda["keV"]) in G

    assert (oda["emax_keV_lower_limit_15integer_upper_limit_1000integer"], a, oda["keV"]) in G
    assert (oda["emax_keV_lower_limit_15integer_upper_limit_1000integer"], oda["lower_limit"], rdflib.Literal(15)) in G

    

        
