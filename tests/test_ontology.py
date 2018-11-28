import os
import logging

test_notebook_repo=os.environ.get('TEST_NOTEBOOK_REPO')

#logger=logging.getLogger("nb2workflow")

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger=logging.getLogger("nb2workflow")
logger.setLevel(level=logging.DEBUG)

def test_nbadapter_repo():
    from nb2workflow.nbadapter import NotebookAdapter, find_notebooks
    from nb2workflow import ontology

    nbas=find_notebooks(test_notebook_repo)

    logger.info(ontology.service_semantic_signature(nbas))

        
