import os
import logging

#logger=logging.getLogger("nb2workflow")

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger("nb2workflow")
logger.setLevel(level=logging.DEBUG)


def test_nb(test_notebook):
    import nb2workflow.nbadapter
    
    basename = os.path.basename(test_notebook).replace(".ipynb","_output")

    print("basename:", basename)

    for p in ".ipynb", ".json", ".html":
        print("base and prefix", basename+p)
        if os.path.exists(basename+p):
            os.remove(basename+p)

    r = nb2workflow.nbadapter.nbrun(test_notebook, {})



    for p in ".ipynb", ".json", ".html":
        assert os.path.exists(basename+p)

    assert 'output_notebook' not in r
    assert 'output_notebook_content' not in r
    assert 'output_notebook_html' in r
    assert 'output_notebook_html_content' in r
    

def test_nb_fail(test_notebook):
    import nb2workflow.nbadapter

    try:
        nb2workflow.nbadapter.nbrun(test_notebook, dict(emin=-1))
    except Exception as e:
        print("got", e)
        assert os.path.exists(os.path.dirname(test_notebook).replace(".ipynb","_output.ipynb"))
        assert os.path.exists(os.path.dirname(test_notebook).replace(".ipynb","_exceptions.json"))
    else:
        raise Exception("failing notebook did not raise exception!")

    


def test_nb_version(test_notebook):
    import nb2workflow.nbadapter
    import rdflib
    
    basename = os.path.basename(test_notebook).replace(".ipynb","_output")

    print("basename:", basename)    

    for p in ".ipynb", ".json", ".html":
        print("base and prefix", basename+p)
        if os.path.exists(basename+p):
            os.remove(basename+p)

    
    nba = nb2workflow.nbadapter.NotebookAdapter(test_notebook)


    assert nba.nb_uri == rdflib.URIRef('http://odahub.io/ontology#workflow-notebook_cf24cd75')

    nba.extract_parameters()

    import rdflib
    G = rdflib.Graph()
    G.parse(data=nba.extra_ttl)

    ttl = G.serialize(format='turtle')
    print(ttl)
    
    versions = list(G.objects(nba.nb_uri, rdflib.URIRef("http://odahub.io/ontology#version")))

    assert versions != ["v1"]



def test_nb_autocollect(test_notebook):
    import nb2workflow.nbadapter
    
    basename = os.path.basename(test_notebook).replace(".ipynb","_output")

    print("basename:", basename)

    for p in ".ipynb", ".json", ".html":
        print("base and prefix", basename+p)
        if os.path.exists(basename+p):
            os.remove(basename+p)

    
    nba = nb2workflow.nbadapter.NotebookAdapter(test_notebook)
    nba.extract_parameters()

    import rdflib
    G = rdflib.Graph()
    G.parse(data=nba.extra_ttl)

    ttl = G.serialize(format='turtle')
    print(ttl)
    
    versions = G.subject_objects(rdflib.URIRef("http://odahub.io/ontology#version"))
    assert versions != []


    r = nb2workflow.nbadapter.nbrun(test_notebook, {})


    for p in ".ipynb", ".json", ".html":
        assert os.path.exists(basename+p)

    assert 'output_notebook' not in r
    assert 'output_notebook_content' not in r
    assert 'output_notebook_html' in r
    assert 'output_notebook_html_content' in r