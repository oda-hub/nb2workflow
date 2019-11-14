import os
import pytest
import base64
import os
import logging

test_notebook=os.environ.get('TEST_NOTEBOOK')
test_notebook_repo=os.environ.get('TEST_NOTEBOOK_REPO')

#logger=logging.getLogger("nb2workflow")

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger("nb2workflow")
logger.setLevel(level=logging.DEBUG)


def test_nb():
    import nb2workflow.nbadapter
    nb2workflow.nbadapter.nbrun(test_notebook, {})

    assert os.path.exists(os.path.dirname(test_notebook).replace(".ipynb","_output.ipynb"))
    assert os.path.exists(os.path.dirname(test_notebook).replace(".ipynb","_output.json"))
    assert os.path.exists(os.path.dirname(test_notebook).replace(".ipynb","_output.html"))

def test_nb_fail():
    import nb2workflow.nbadapter

    try:
        nb2workflow.nbadapter.nbrun(test_notebook, dict(emin=-1))
    except Exception as e:
        print("got", e)
        assert os.path.exists(os.path.dirname(test_notebook).replace(".ipynb","_output.ipynb"))
        assert os.path.exists(os.path.dirname(test_notebook).replace(".ipynb","_exceptions.json"))
    else:
        raise Exception("failing notebook did not raise exception!")

    
