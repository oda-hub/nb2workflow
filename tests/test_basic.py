import os

test_notebook=os.environ.get('TEST_NOTEBOOK')

def test_nbadapter():
    from nb2workflow.nbadapter import NotebookAdapter

    nba=NotebookAdapter(test_notebook)
    parameters=nba.extract_parameters()

    print(parameters)
    assert len(parameters)==2

