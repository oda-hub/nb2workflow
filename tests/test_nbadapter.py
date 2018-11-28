import os
import logging

test_notebook=os.environ.get('TEST_NOTEBOOK')
test_notebook_repo=os.environ.get('TEST_NOTEBOOK_REPO')

#logger=logging.getLogger("nb2workflow")

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger("nb2workflow")
logger.setLevel(level=logging.DEBUG)

def test_nbadapter():
    from nb2workflow.nbadapter import NotebookAdapter

    nba=NotebookAdapter(test_notebook)
    parameters=nba.extract_parameters()

    print(parameters)
    assert len(parameters)==4

    assert 'comment' in parameters['scwid']
    assert parameters['scwid']['owl_type'] == "http://odahub.io/ontology/integral#ScWID"


    outputs = nba.extract_output_declarations()
    print("outputs",outputs)
    
    assert len(outputs) == 3

    if os.path.exists(nba.output_notebook_fn):
        os.remove(nba.output_notebook_fn)
    
    if os.path.exists(nba.preproc_notebook_fn):
        os.remove(nba.preproc_notebook_fn)

    nba.execute(dict())

    output=nba.extract_output()

    print(output)
    assert len(output)==4

    assert 'spectrum' in output
    

def test_nbadapter_repo():
    from nb2workflow.nbadapter import NotebookAdapter, find_notebooks

    nbas=find_notebooks(test_notebook_repo)

    assert len(nbas) == 1

    for nba_name,nba in nbas.items():
        print("notebook",nba_name)
        
        continue
        parameters=nba.extract_parameters()

        print(parameters)
        assert len(parameters)==4

        if os.path.exists(nba.output_notebook_fn):
            os.remove(nba.output_notebook_fn)

        if os.path.exists(nba.preproc_notebook_fn):
            os.remove(nba.preproc_notebook_fn)

        nba.execute(dict())

        output=nba.extract_output()

        print(output)
       # assert len(output)==1

       # assert 'spectrum' in output
        
