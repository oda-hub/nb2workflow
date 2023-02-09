import os
import subprocess

import pytest


@pytest.mark.cwl
def test_cwl(test_notebook):
    cwl_fn = "test.cwl"
    import nb2workflow.cwl as cwl

    cwl.nb2cwl(test_notebook, cwl_fn)

    subprocess.check_call(["cwltool", cwl_fn])


@pytest.mark.skip("not testing odakb")
@pytest.mark.cwl
def test_cwl_odakb(test_notebook):
    cwl_fn = "test.cwl"
    import nb2workflow.cwl as cwl

    cwl.nb2cwl(test_notebook, "test.cwl", nbrunner_module="odakb.evaluator")

    subprocess.check_call(["cwltool", cwl_fn])
