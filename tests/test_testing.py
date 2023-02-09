from nb2workflow.testing import nbtest


def test_simpletesting(test_notebook_repo):
    from nb2workflow.testing import nbtest

    nbtest(test_notebook_repo)

    # nbas = find_notebooks(test_notebook_repo)

    # assert len(nbas) == 1

    # for nba_name, nba in nbas.items():
    #     print("notebook", nba_name)

    #     parameters = nba.extract_parameters()

    #     print(parameters)
    #     assert len(parameters) == 6

    #     if os.path.exists(nba.output_notebook_fn):
    #         os.remove(nba.output_notebook_fn)

    #     if os.path.exists(nba.preproc_notebook_fn):
    #         os.remove(nba.preproc_notebook_fn)

    #     nba.execute(dict())

    #     output = nba.extract_output()

    #     print(output)

    #     assert len(output) == 4
    #     assert 'spectrum' in output