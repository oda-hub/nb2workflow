import os
import json
import logging
import pytest


# this can be also set in pytest call
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger("nb2workflow")
logger.setLevel(level=logging.DEBUG)

def _mimick_convert_minor_version(nb_fn, version):
    j = json.load(open(nb_fn))
    j['nbformat_minor'] = version

    fn = nb_fn.replace(".ipynb", f"_mock_downgraded_to_{version}.ipynb")

    json.dump(j, open(fn, "w"))

    return fn

@pytest.mark.parametrize("morph_notebook", ["mimick_convert_minor_version", "vanilla"])
def test_nbadapter(test_notebook, morph_notebook, caplog):
    from nb2workflow.nbadapter import NotebookAdapter

    if morph_notebook == "mimick_convert_minor_version":
        fn = _mimick_convert_minor_version(test_notebook, 2)
    elif morph_notebook == "vanilla":
        fn = test_notebook
    else:
        raise NotImplementedError


    nba = NotebookAdapter(fn)
    parameters = nba.extract_parameters()

    for k, v in parameters.items():
        print("\033[31m", k, ":", v, "\033[0m")

    assert len(parameters) == 6

    assert 'comment' in parameters['scwid']
    assert parameters['scwid']['owl_type'] == "http://odahub.io/ontology/integral#ScWID"

    assert parameters['enabled']['owl_type'] == "http://odahub.io/ontology#Boolean"

    outputs = nba.extract_output_declarations()
    print("outputs", outputs)

    assert len(outputs) == 4

    if os.path.exists(nba.output_notebook_fn):
        os.remove(nba.output_notebook_fn)

    if os.path.exists(nba.preproc_notebook_fn):
        os.remove(nba.preproc_notebook_fn)

    nba.execute(dict())

    output = nba.extract_output()

    if morph_notebook == "mimick_convert_minor_version":
        assert 'will attempt to convert, but expect other warnings' in caplog.text

        os.remove(fn)

    print(output)
    assert len(output) == 6

    assert 'spectrum' in output

def test_find_notebooks(caplog):
    from nb2workflow.nbadapter import find_notebooks, NotebookAdapter
    testfiles_path = os.path.join(os.path.dirname(__file__), 'testfiles')
    nb_dir = testfiles_path
    single_nb = os.path.join(testfiles_path, 'lightcurve.ipynb')
    
    nbas = find_notebooks(single_nb)
    assert len(nbas) == 1
    
    nbas = find_notebooks(single_nb, pattern=r'.*bool')
    assert len(nbas) == 1
    assert 'Ignoring pattern.' in caplog.text
    
    nbas = find_notebooks(nb_dir)
    assert len(nbas) == 9
    
    nbas = find_notebooks(nb_dir, pattern=r'.*bool')
    assert len(nbas) == 1
    

def test_nbadapter_repo(test_notebook_repo):
    from nb2workflow.nbadapter import NotebookAdapter, find_notebooks, validate_oda_dispatcher

    nbas = find_notebooks(test_notebook_repo)

    assert len(nbas) == 1

    for nba_name, nba in nbas.items():
        print("notebook", nba_name)

        parameters = nba.extract_parameters()

        print(parameters)
        assert len(parameters) == 6

        if os.path.exists(nba.output_notebook_fn):
            os.remove(nba.output_notebook_fn)

        if os.path.exists(nba.preproc_notebook_fn):
            os.remove(nba.preproc_notebook_fn)

        nba.execute(dict())

        output = nba.extract_output()

        print(output)

        assert len(output) == 6
        assert 'spectrum' in output

        validate_oda_dispatcher(nba)

@pytest.mark.skip(reason="Reproducing this condition in the test is difficult")
def test_nbadapter_lfs_repo(test_notebook_lfs_repo):
    from nb2workflow.nbadapter import NotebookAdapter, find_notebooks, validate_oda_dispatcher

    nbas = find_notebooks(test_notebook_lfs_repo)

    assert len(nbas) >= 1

    for nba_name, nba in nbas.items():
        print("notebook", nba_name)

        if os.path.exists(nba.output_notebook_fn):
            os.remove(nba.output_notebook_fn)

        if os.path.exists(nba.preproc_notebook_fn):
            os.remove(nba.preproc_notebook_fn)

        try:
            nba.execute(dict())
            assert False, 'nba.execute is expected to fail'
        except Exception as ex:
            assert ex.message == "git-lfs is not initialized"
        break

def test_nbreduce(test_notebook):
    from nb2workflow.nbadapter import NotebookAdapter, nbreduce, setup_logging

    setup_logging()

    nba = NotebookAdapter(test_notebook)

    if os.path.exists(nba.output_notebook_fn):
        os.remove(nba.output_notebook_fn)

    if os.path.exists(nba.preproc_notebook_fn):
        os.remove(nba.preproc_notebook_fn)

    nba.execute(dict())

    output = nba.extract_output()

    assert len(output) == 6

    assert 'spectrum' in output

    print("will reduce", nba.output_notebook_fn)

    nbsize_b = os.path.getsize(nba.output_notebook_fn)

    assert isinstance(nbsize_b, int)

    assert nbsize_b > 0

    nbreduce(nba.output_notebook_fn, nbsize_b/1024./1024 + 1.)

    nbreduce(nba.output_notebook_fn, nbsize_b/1024./1024*0.5)


def test_denumpyfy():
    import numpy as np
    import json
    from nb2workflow.nbadapter import denumpyfy

    data = {"d": np.bool_(True), "k": np.array([1, 2, 3]), "dd": [
        1, 2, np.float32(33), {'a': np.int64(10)}]}

    try:
        r = json.dumps(data)
        print(r)
    except TypeError as e:
        print("failed as expected", e)
    else:
        raise Exception("did not fail")

    r = json.dumps(denumpyfy(data))
    print(r)


def test_multiline_parameters():
    from nb2workflow.nbadapter import NotebookAdapter

    nba = NotebookAdapter('tests/testfiles/multiline.ipynb')

    pars = nba.input_parameters

    assert pars['mline']['python_type'] == dict
    assert pars['mline']['default_value'] == {'foo': ['bar', 'baz'],
                                              'spam': ['ham', 'eggs']}
    assert pars['mline']['owl_type'] == "http://odahub.io/ontology#StructuredParameter"
    assert pars['mline']['is_optional'] == False


    assert pars['opt']['python_type'] == float
    assert pars['opt']['default_value'] == None
    assert pars['opt']['owl_type'] == "http://odahub.io/ontology#Float"
    assert pars['opt']['is_optional'] == True


    assert pars['inten']['python_type'] == int
    assert pars['inten']['default_value'] == 45
    assert pars['inten']['owl_type'] == "http://odahub.io/ontology#Energy"
    assert pars['inten']['is_optional'] == False

    assert pars['intfloat']['python_type'] == float
    assert pars['intfloat']['default_value'] == 10
    assert pars['intfloat']['owl_type'] == "http://odahub.io/ontology#Float"
    assert pars['intfloat']['is_optional'] == False

    assert pars['string_param']['python_type'] == str
    assert pars['string_param']['default_value'] == 'Foo Bar\nContains = Symbol\nSpam Ham\n'
    assert pars['string_param']['owl_type'] == "http://odahub.io/ontology#LongString"
    assert pars['string_param']['is_optional'] == False

    assert pars['flag']['python_type'] == bool
    assert pars['flag']['default_value'] == True
    assert pars['flag']['owl_type'] == "http://odahub.io/ontology#Boolean"
    assert pars['flag']['is_optional'] == False

    outp = nba.extract_output_declarations()

    assert outp['static']['value'] == "Just a static\n            but multiline string"
