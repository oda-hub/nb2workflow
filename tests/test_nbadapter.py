import os
import json
import logging
import pytest
import tempfile


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

    assert parameters['enabled']['owl_type'] == "http://www.w3.org/2001/XMLSchema#bool"

    outputs = nba.extract_output_declarations()
    print("outputs", outputs)

    assert len(outputs) == 3

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
    assert len(output) == 4

    assert 'spectrum' in output


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

        assert len(output) == 4
        assert 'spectrum' in output

        validate_oda_dispatcher(nba)


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

    assert len(output) == 4

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
