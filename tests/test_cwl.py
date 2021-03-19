import os
import subprocess

import pytest

import nb2workflow.cwl as cwl

@pytest.mark.xfail
def test_cwl(test_notebook):
    cwl_fn = "test.cwl"

    cwl.nb2cwl(test_notebook, cwl_fn)

    subprocess.check_call(["cwl-runner", cwl_fn])


@pytest.mark.xfail
def test_cwl_odakb(test_notebook):
    cwl_fn = "test.cwl"

    cwl.nb2cwl(test_notebook, "test.cwl", nbrunner_module="odakb.evaluator")

    subprocess.check_call(["cwl-runner", cwl_fn])
