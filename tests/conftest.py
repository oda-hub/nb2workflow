import re
import os
import shutil
import pytest
import signal
import psutil
import subprocess
import tempfile
import requests

import nb2workflow.service
from importlib import reload
import rdflib as rdf

from oda_api.ontology_helper import ODA, ODAS

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


@pytest.fixture
def app_low_download_limit():
    testfiles_path = os.path.join(os.path.dirname(__file__), 'testfiles')
    app_low_download_limit = nb2workflow.service.app
    app_low_download_limit.notebook_adapters = nb2workflow.nbadapter.find_notebooks(testfiles_path)
    for nb, nba_obj in app_low_download_limit.notebook_adapters.items():
        nba_obj.max_download_size = 1
    nb2workflow.service.setup_routes(app_low_download_limit)
    print("creating app with low limit on the download of files")
    return app_low_download_limit


# TODO improve this, as it requires changes also in the oda_api
@pytest.fixture
def app_not_available_ontology():
    nb2workflow.nbadapter.ontology._is_ontology_available = False
    yield



def kill_child_processes(parent_pid, sig=signal.SIGTERM):
    try:
        parent = psutil.Process(parent_pid)
        children = parent.children(recursive=True)
        for process in children:
            process.send_signal(sig)
    except psutil.NoSuchProcess:
        return


def download_file(url, local_filename=None):
    if local_filename is None:
        local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk:
                f.write(chunk)
    return local_filename


@pytest.fixture(scope="module")
def temp_dir(request):
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture(scope="module")
def ontology_path(temp_dir):
    ontology_url = "https://raw.githubusercontent.com/oda-hub/ontology/main/ontology.ttl"
    ontology_path = os.path.join(temp_dir, "ontology.ttl")
    download_file(ontology_url, ontology_path)
    # subprocess.check_call(["wget", ontology_url, "-O", ontology_path])
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
