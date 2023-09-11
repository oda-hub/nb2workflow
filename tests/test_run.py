import os
import logging

import pytest

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


    assert nba.nb_uri == rdflib.URIRef('http://odahub.io/ontology#workflow-notebook_5daa7d90')

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


@pytest.mark.parametrize("limit", [None, 1, 1e6])
def test_nb_attach_file(test_notebook, limit):
    import nb2workflow.nbadapter
    
    basename = os.path.basename(test_notebook).replace(".ipynb","_output")

    print("basename:", basename)

    for p in ".ipynb", ".json", ".html":
        print("base and prefix", basename + p)
        if os.path.exists(basename + p):
            os.remove(basename + p)

    nba = nb2workflow.nbadapter.NotebookAdapter(test_notebook)

    # all content attached
    nba.limit_output_attachment_file = limit

    nba.execute({})
    r = nba.extract_output()

    # for p in ".ipynb", ".json", ".html":
    #     assert os.path.exists(basename+p)

    assert 'spectrum_png' in r

    if limit is None or limit > 10:
        assert 'spectrum_png_content' in r
    else:
        assert 'spectrum_png_url' in r
        assert "cache/nb2workflow/bigoutputs" in r['spectrum_png_url']

        assert r['energies_fits_file'] == "energies.fits"
        assert "cache/nb2workflow/bigoutputs" in r['energies_fits_file_url']