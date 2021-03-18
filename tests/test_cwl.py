import nb2workflow.cwl as cwl
import os

def test_cwl():
    cwl_fn="test.cwl"    

    cwl.nb2cwl(os.environ.get("TEST_NOTEBOOK"), "test.cwl")

    import subprocess

    subprocess.check_call(["cwl-runner", "test.cwl"])

def test_cwl_odakb():
    cwl_fn="test.cwl"    

    cwl.nb2cwl(os.environ.get("TEST_NOTEBOOK"), "test.cwl", nbrunner_module="odakb.evaluator")

    import subprocess

    subprocess.check_call(["cwl-runner", "test.cwl"])
