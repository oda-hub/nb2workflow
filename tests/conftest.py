import re
import os
import shutil
import pytest
import signal
import psutil
import subprocess
import tempfile

import nb2workflow.service


@pytest.fixture
def test_notebook():
    return os.environ.get('TEST_NOTEBOOK',
                          os.path.join(os.getcwd(), "tests/testrepo/workflow-notebook.ipynb"))

@pytest.fixture
def test_notebook_old():
    return os.environ.get('TEST_NOTEBOOK',
                          os.path.join(os.getcwd(), "tests/testrepo/workflow-notebook.ipynb"))

@pytest.fixture
def test_notebook_repo():
    path = os.environ.get('TEST_NOTEBOOK_REPO', None)
    
    if path is None:
        path = os.path.join(os.getcwd(), 'tests/testrepo/')
        if os.path.exists(path):
            shutil.rmtree(path)
        subprocess.check_call(["git", "clone", "https://github.com/volodymyrss/nbworkflow-test.git", path])

    return path


@pytest.fixture
def test_notebook_lfs_repo():
    path = os.environ.get('TEST_NOTEBOOK_LFS_REPO', None)

    if path is None:
        path = os.path.join(os.getcwd(), 'tests/testlfsrepo/')
        if os.path.exists(path):
            shutil.rmtree(path)
        subprocess.check_call(["git", "clone", "https://gitlab.renkulab.io/astronomy/mmoda/crbeam.git", path])

    return path

@pytest.fixture
def app(test_notebook):
    app = nb2workflow.service.app
    app.notebook_adapters = nb2workflow.nbadapter.find_notebooks(test_notebook)
    nb2workflow.service.setup_routes(app)
    print("creating app")
    return app


def kill_child_processes(parent_pid, sig=signal.SIGTERM):
    try:
        parent = psutil.Process(parent_pid)
        children = parent.children(recursive=True)
        for process in children:
            process.send_signal(sig)
    except psutil.NoSuchProcess:
        return


@pytest.fixture(scope="module")
def temp_dir(request):
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture(scope="module")
def ontology_path(temp_dir):
    ontology_url = "https://raw.githubusercontent.com/oda-hub/ontology/main/ontology.ttl"
    ontology_path = os.path.join(temp_dir, "ontology.ttl")
    subprocess.check_call(["wget", ontology_url, "-O", ontology_path])
    yield ontology_path

@pytest.fixture
def service_fixture(pytestconfig, test_notebook_repo):
    import subprocess
    import os
    import copy
    import time
    from threading import Thread

    env = copy.deepcopy(dict(os.environ))
    print("rootdir", str(pytestconfig.rootdir))
    env['PYTHONPATH'] = str(pytestconfig.rootdir)+":" + \
        str(pytestconfig.rootdir)+"/tests:"+env.get('PYTHONPATH', "")
    print("pythonpath", env['PYTHONPATH'])

    p = subprocess.Popen(
        ["python", "-m", "nb2workflow.service",
            test_notebook_repo, '--port', '9292'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
        env=env,
    )

    url_store = [None]

    def follow_output():
        url_store[0] = None
        for line in iter(p.stdout.readline, b''):
            line = line.decode()

            print("following server:", line.rstrip())
            m = re.search("Running on (.*?) \(Press CTRL\+C to quit\)", line)
            if m:
                # alaternatively get from configenv
                url_store[0] = m.group(1)[:-1]
                print("found url:", url_store[0])

            if re.search("\* Debugger PIN:.*?", line):
                url_store[0] = url_store[0].replace("0.0.0.0", "127.0.0.1")
                print("server ready, url", url_store[0])

    thread = Thread(target=follow_output, args=())
    thread.start()

    while url_store[0] is None:
        time.sleep(0.1)
    time.sleep(0.5)

    ddservice = url_store[0]

    yield ddservice

    print("child:", p.pid)
    # p.kill()

    kill_child_processes(p.pid, signal.SIGKILL)
    os.kill(p.pid, signal.SIGKILL)


def pytest_addoption(parser):
    parser.addoption(
        "--runservice", action="store_true", default=False, help="run service tests"
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "service: mark test as relying on local service to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runservice"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_service = pytest.mark.skip(reason="need --runservice option to run")
    for item in items:
        if "service" in item.keywords:
            item.add_marker(skip_service)
