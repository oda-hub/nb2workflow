from __future__ import annotations
# for py3.9

import pickle
import re
import os
import json
import glob
import time
import logging
from typing import Any
import requests
from requests.auth import HTTPBasicAuth
import base64
import hashlib
import datetime
import tempfile
import nbformat
import yaml
import traceback
from dataclasses import dataclass, field

from werkzeug.routing import RequestRedirect
from werkzeug.exceptions import MethodNotAllowed, NotFound

import queue
from nb2workflow import ontology, publish, schedule
from nb2workflow.nbadapter import NotebookAdapter, find_notebooks, PapermillWorkflowIncomplete

from io import BytesIO
from bs4 import BeautifulSoup


from flask import Flask, Blueprint, make_response, jsonify, request, url_for, send_file, Response, current_app
from flask_caching import Cache
from flask.json.provider import DefaultJSONProvider

from flasgger import LazyString, Swagger, swag_from

from nb2workflow.helpers import serialize_workflow_exception
from nb2workflow.json import CustomJSONEncoder

import threading

verify_tls = False


logger = logging.getLogger('nb2workflow.service')


async_queue = queue.Queue()


class CustomJSONProvider(DefaultJSONProvider):
    def dumps(self, obj: Any, **kwargs: Any) -> str:
        kwargs['cls'] = CustomJSONEncoder
        return json.dumps(obj, **kwargs)

class ReverseProxied(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        script_name = environ.get('HTTP_X_FORWARDED_PREFIX', '')
        if script_name:
            environ['SCRIPT_NAME'] = script_name
            path_info = environ['PATH_INFO']
            if path_info.startswith(script_name):
                environ['PATH_INFO'] = path_info[len(script_name):]

        scheme = environ.get('HTTP_X_SCHEME', '')
        if scheme:
            environ['wsgi.url_scheme'] = scheme
        return self.app(environ, start_response)


cache = Cache(config={'CACHE_TYPE': 'simple'})
blprint = Blueprint('nb2w', __name__)


@dataclass
class WfStateGlobalVarStorage:
    async_workflows: dict[str, dict | str] = field(default_factory=dict)
    async_workflow_jobdirs: dict[str, str] = field(default_factory=dict)
    service_semantic_signature: str = ''
    notebook_adapters: dict[str, NotebookAdapter] = field(default_factory=dict)

    _lock: threading.RLock = field(default_factory=threading.RLock)

    def reset(self):
        self.async_workflows = {}
        self.async_workflow_jobdirs = {}
        self.service_semantic_signature = ''
        self.notebook_adapters = {}

    def async_reset(self):
        with self._lock:
            self.async_workflows = {}
            self.async_workflow_jobdirs = {}


wfstore = WfStateGlobalVarStorage()



def setup_routes(app: Flask):
    for target, nba in wfstore.notebook_adapters.items():
        target_specs = {
            "parameters": [
                {
                    "name": p_name,
                    "in": "query",
                    "schema": {
                        "type": to_oapi_type(p_data['python_type']),
                        "nullable": p_data.get('is_optional', False)
                        # strictly speaking, there is no way to set query parameter to null
                        # we define a convention in which '%00' string represents null
                        },
                    "required": False,
                    "default": p_data['default_value'],
                    "description": p_data['comment']+" "+p_data['owl_type'],
                }
                for p_name, p_data in nba.extract_parameters().items()
            ],
            "responses": {
                "200": {
                    "description": repr(nba.extract_output_declarations()),
                }
            }
        }

        endpoint = 'endpoint_'+target

        def funcg(target):
            def workflow_func():
                return workflow(target)
            return workflow_func

        logger.debug("target: %s with endpoint %s", target, endpoint)

        def response_filter(rv):
            if isinstance(rv, tuple) and isinstance(rv[0], Response) and rv[1] != 200:
                logger.info("NOT caching response %s", rv[1])
                return False
            elif isinstance(rv, Response) and rv.status != 200:
                logger.info("NOT caching response %s", rv)
                return False
            else:
                logger.info("should cache response %s", rv)                
                try:
                    pickle.dumps(rv)
                except pickle.PicklingError as e:
                    logger.info("the response can not be pickled and cached %s", e)
                    return False

                return True

        cache_timeout = nba.get_system_parameter_value('cache_timeout', 0)
        try:
            app.route('/api/v1.0/get/'+target, methods=['GET'], endpoint=endpoint)(
                swag_from(target_specs)(
                    cache.cached(timeout=cache_timeout, response_filter=response_filter, query_string=True)(
                        funcg(target)
                    )))
        except AssertionError as e:
            logger.warning("unable to add route: %s, ignoring the endpoint", e)
            continue

        schedule_interval = nba.get_system_parameter_value(
            'schedule_interval', 0)
        if schedule_interval > 0:
            logger.info("scheduling callable %s every %lg",
                        str(funcg), float(schedule_interval))

            def schedulable():
                with app.test_request_context('/api/v1.0/get/'+target):
                    from flask import request
                    get_view_function('/api/v1.0/get/'+target)[0]()

            schedule.schedule_callable(schedulable, schedule_interval)


def create_app():
    app = Flask(__name__)

    app.config['SWAGGER'] = {
        'uiversion': 3,
        "openapi": "3.0.3",
    }
    template = {
        "swaggerUiPrefix": LazyString(lambda: request.environ.get('HTTP_X_FORWARDED_PREFIX', '')),
        "info": {
            "title": "ODAHub API",
            "description": "",
            "contact": {
                "responsibleOrganization": "ODA",
                "responsibleDeveloper": "Volodymyr SAVCHENKO",
                "email": "volodymyr.savchenko@unige.ch",
                "url": "https://odahub.io",
            },
            "termsOfService": "http://me.com/terms",
            "version": "0.0.1"
        }
    }
    Swagger(app, template=template)
    app.wsgi_app = ReverseProxied(app.wsgi_app)
    app.json = CustomJSONProvider(app)
    app.config["JSON_SORT_KEYS"] = False
    cache.init_app(app, config={'CACHE_TYPE': 'SimpleCache'})
    app.register_blueprint(blprint)

    with wfstore._lock:
        if wfstore.notebook_adapters:
            setup_routes(app)

    app.config['STARTED_AT'] = datetime.datetime.now()

    return app



class AsyncWorker(threading.Thread):
    def __init__(self, worker_id):
        self.worker_id = worker_id
        super(AsyncWorker, self).__init__()

    def run(self):
        while True:
            self.run_one()
            time.sleep(5)

    def run_one(self):
        logger.info("worker_id %s", self.worker_id)
        async_workflow = async_queue.get(block=True)
        async_workflow.run()

class AsyncWorkflow:
    def __init__(self, key, target, params, context={}):
        self.key = key
        self.target = target
        self.params = params
        self.context = context

        logger.info("%s initializing callback %s", self, self.callback)

    @property
    def callback(self):
        return self.context.get('callback', None)


    def run(self):
        try:
            self._run()
        except Exception as e:
            logger.error("run failed inexplicably: %s\n%s", repr(e), traceback.format_exc())
            with wfstore._lock:
                wfstore.async_workflows[self.key] = dict(
                    output={}, 
                    exceptions=[serialize_workflow_exception(e)]
                )

    def note(self, *args, **kwargs):
        if not hasattr(self, 'notes'):
            self.notes = []

        self.notes.append(dict(
            time=time.time(),
            data=(args, kwargs),
        ))

    blocked_until = 0

    def _run(self):
        if self.blocked_until > time.time():
            logger.info("workflow still blocked, waiting %i",
                        self.blocked_until - time.time())
            async_queue.put(self)
            with wfstore._lock:
                wfstore.async_workflows[self.key] = 'submitted'
            return

        with wfstore._lock:
            template_nba = wfstore.notebook_adapters.get(self.target)
            nba = NotebookAdapter(template_nba.notebook_fn, tempdir_cache=wfstore.async_workflow_jobdirs,
                                n_download_max_tries=template_nba.n_download_max_tries,
                                download_retry_sleep_s=template_nba.download_retry_sleep_s,
                                max_download_size=template_nba.max_download_size)
            
            wfstore.async_workflows[self.key] = 'started'
        self.perform_callback(action='progress')

        try:
            thread_id = threading.get_ident()
            process_id = os.getpid()
            logger.info(f'nba.execute thread id: {thread_id} ; process id: {process_id}')
            exceptions = nba.execute(self.params['request_parameters'], context=self.context, tmpdir_key=self.key)
        except PapermillWorkflowIncomplete as e:
            logger.info("found incomplete workflow: %s, rescheduling", repr(e))

            self.note("rescheduled")

            self.blocked_until = time.time() + 10

            async_queue.put(self)
            with wfstore._lock:
                wfstore.async_workflows[self.key] = 'submitted'
            return

        logger.info("exceptions: %s", repr(exceptions))

        if len(exceptions) > 0:
            output = 'incomplete'
            logger.error("exceptions: %s", repr(exceptions))
        else:
            nretry = 10
            while nretry > 0:
                try:
                    output = nba.extract_output()
                    logger.info("completed, output length %s", len(output))
                    if len(output) == 0:
                        logger.debug(
                            "output from notebook is empty, something failed, attempts left: %s", nretry)
                    else:
                        break
                except nbformat.reader.NotJSONError as e:
                    logger.debug(
                        "output notebook incomplete %s attempts left: %s", e, nretry)
                except Exception as e:
                    logger.debug(
                        "output notebook incomplte or does not exist %s attempts left: %s", e, nretry)

                nretry -= 1
                time.sleep(1)

        logger.debug("output: %s", output)

        logger.info("updating key %s", self.key)
        with wfstore._lock:
            wfstore.async_workflows[self.key] = dict(output=output, exceptions=list(
                map(serialize_workflow_exception, exceptions)), jobdir=nba.tmpdir)

        self.perform_callback()

    def perform_callback(self, action='done'):
        if self.callback is None:
            logger.info('no callback registered, skipping')
            return

        logger.info('will perform callback: %s', self.callback)

        callback_payload = dict(
            action=action
        )
        
        if re.match('^file://', self.callback):
            with open(self.callback.replace('file://', ''), "w") as f:
                 json.dump(callback_payload, f)
            logger.info('stored callback in a file %s', self.callback)

        elif re.match('^https?://', self.callback):
            r = requests.get(self.callback, params=callback_payload)
            logger.info('callback %s returns %s : %s', self.callback, r, r.text)
        
        else:
            raise NotImplementedError


def workflow(target, background=False, async_request=False):
    issues = []

    async_request = request.args.get('_async_request', async_request)
    async_request_callback = request.args.get('_async_request_callback', None)
    token = request.args.get("_token", None)
    context = dict(callback=async_request_callback, token=token)

    logger.debug("target %s", target)

    if not background:
        logger.debug("raw parameters %s", request.args)

    with wfstore._lock:
        template_nba = wfstore.notebook_adapters.get(target)
    
        nba = NotebookAdapter(template_nba.notebook_fn,
                            n_download_max_tries=template_nba.n_download_max_tries,
                            download_retry_sleep_s=template_nba.download_retry_sleep_s,
                            max_download_size=template_nba.max_download_size)

        if nba is None:
            interpreted_parameters = None
            issues.append("target not known: %s; available targets: %s" %
                        (target, wfstore.notebook_adapters.keys()))
        else:
            if not background:
                interpreted_parameters = nba.interpret_parameters(request.args)
                issues += interpreted_parameters['issues']
            else:
                interpreted_parameters = dict(request_parameters=[])

        logger.debug("interpreted parameters %s", interpreted_parameters)

    # async
    if async_request:
        if len(issues) > 0:
            exceptions = [serialize_workflow_exception(ValueError(v)) for v in issues]
            return make_response(jsonify(workflow_status="done",
                                        data=dict(output="incomplete", 
                                                    exceptions=exceptions),
                                        comment=""), 200)

        key = hashlib.sha224(json.dumps(
            dict(target=target, params=interpreted_parameters)).encode('utf-8')).hexdigest()

        with wfstore._lock:
            value = wfstore.async_workflows.get(key, None)

            print('cache key/value', key, value)

            if value is None:
                async_task = AsyncWorkflow(key=key,
                                        target=target,
                                        params=interpreted_parameters,
                                        context=context
                                        )
                
                wfstore.async_workflows[key] = 'submitted'
                async_queue.put(async_task)

                return make_response(jsonify(workflow_status="submitted",
                                            comment="task created"),
                                    201)

            elif value == 'submitted':
                return make_response(jsonify(workflow_status=value,
                                            comment=f"task is {value}"),
                                    201)

            elif value == 'started':
                return make_response(jsonify(workflow_status=value,
                                            comment=f"task is {value}",
                                            jobdir=wfstore.async_workflow_jobdirs.get(key)),
                                    201)

            else:
                return make_response(jsonify(workflow_status="done",
                                            data=value,
                                            comment=""),
                                    200)

    if len(issues) > 0:
        return make_response(jsonify(issues=issues), 400)
    else:
        exceptions = nba.execute(interpreted_parameters['request_parameters'], context=context)

        if not os.path.exists(nba.output_notebook_fn):
            output = {}
        else:
            nretry = 10
            while nretry > 0:
                try:
                    output = nba.extract_output()
                    if len(output) == 0:
                        logger.debug(
                            "output from notebook is empty, something failed, attempts left: %s", nretry)
                    else:
                        break
                except nbformat.reader.NotJSONError as e:
                    logger.debug(
                        "output notebook incomplte %s attempts left: %s", e, nretry)

                nretry -= 1
                time.sleep(1)

        logger.debug("output: %s", output)
        logger.debug("exceptions: %s", exceptions)

        r = jsonify(dict(
                    output=output,
                    exceptions=[repr(e) for e in exceptions],
                    jobdir=nba.tmpdir,
                    ))

        return_code = 200
        if len(exceptions) > 0:
            return_code = 500

        return r, return_code


def to_oapi_type(in_type):
    if issubclass(in_type, bool):
        out_type = 'boolean'
    
    elif issubclass(in_type, int):
        out_type = 'integer'

    elif issubclass(in_type, float):
        out_type = 'number'

    elif issubclass(in_type, str):
        out_type = 'string'

    elif issubclass(in_type, bool):
        out_type = 'boolean'

    else:
        out_type = 'string'
        logger.debug(f"using default type cast from {in_type}to {out_type}")
    

    logger.debug("oapi type cast from %s to %s", repr(in_type), repr(out_type))

    return out_type


def get_view_function(url, method='GET'):
    """Match a url and return the view and arguments
    it will be called with, or None if there is no view.
    """

    adapter = current_app.url_map.bind('localhost')

    try:
        match = adapter.match(url, method=method)
    except RequestRedirect as e:
        # recursively match redirects
        return get_view_function(e.new_url, method)
    except (MethodNotAllowed, NotFound):
        # no match
        return None

    try:
        # return the view function and arguments
        return current_app.view_functions[match[0]], match[1]
    except KeyError:
        # no view is associated with the endpoint
        return None


@blprint.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers',
                        'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods',
                        'GET,PUT,POST,DELETE,OPTIONS')
    return response

@blprint.route('/api/v1.0/options', methods=['GET'])
def workflow_options():
    with wfstore._lock:
        return jsonify(dict([
            (
                target,
                dict(output=nba.extract_output_declarations(),
                        parameters=nba.extract_parameters()),
                )
            for target, nba in wfstore.notebook_adapters.items()
        ]))


@blprint.route('/api/v1.0/get-<mode>/<target>/<filename>', methods=['GET'])
def workflow_filename(mode, target, filename):

    target_url = url_for('endpoint_'+target, _external=True, **request.args)

    # report equivalency

    if "HTTP_AUTH" in os.environ:
        username, password = os.environ.get("HTTP_AUTH").split(":") # pyright: ignore[reportOptionalMemberAccess]
        auth = HTTPBasicAuth(username, password)
    else:
        auth = None

    r = requests.get(target_url, verify=verify_tls, auth=auth)

    try:
        rj = r.json()
        logger.info(rj.keys())

        if 'data' in rj:
            output = rj.get('data').get('output')
        elif 'output' in rj:
            output = rj.get('output')
        else:
            return jsonify(rj)

        if filename+'_content' in output:
            content = base64.b64decode(output.get(filename+'_content', None))
        else:
            return jsonify({'workflow_status': 'anomaly', 'comment': 'searching for key '+filename+'_content'+', available: '+(", ".join(output.keys())), 'base_workflow_result': rj})

        if content:
            if mode == "file" or mode == "png":
                return send_file(BytesIO(content), mimetype='image/png')
            elif mode == "html":
                return content
            else:
                return 404
        else:
            return jsonify(dict(
                exceptions=["no such file, available:", output.keys()],
            ))

    except Exception as e:
        logger.error("problem decoding: %s", repr(e))
        return jsonify(dict(
            exceptions={"problem decoding": repr(
                e), "raw_response": r.content.decode('utf-8')},
        ))


@blprint.route('/api/v1.0/rdf', methods=['GET'])
def workflow_rdf():
    with wfstore._lock:
        return make_response(wfstore.service_semantic_signature)


@blprint.route('/health')
def healthcheck():
    status, issues = current_health()

    if len(issues) == 0:
        return jsonify(dict(summary="all is ok!", status=status))
    else:
        return make_response(jsonify(issues=issues, status=status, summary="warning: "+"; ".join(issues)), 500)


def current_health():
    issues = []
    status = {}

    statvfs = os.statvfs(".")
    status['fs_space'] = dict(
        size_mb=statvfs.f_frsize * statvfs.f_blocks / 1024 / 1024,
        avail_mb=statvfs.f_frsize * statvfs.f_bavail / 1024 / 1024,
    )

    if status['fs_space']['avail_mb'] < 300:
        issues.append("not enough free space: %.5lg Mb left" %
                      status['fs_space']['avail_mb'])

    import psutil

    processes = []
    status['n_open_files'] = 0
    status['n_processes'] = 0
    status['n_threads'] = 0

    for proc in psutil.process_iter():
        try:
            processes.append(dict(
                n_open_files=len(proc.open_files()),
            ))

            status['n_open_files'] += len(proc.open_files())
            status['n_processes'] += 1
            status['n_threads'] += proc.num_threads()
        except Exception as e:
            pass

    status['cpu_times'] = dict(psutil.cpu_times_percent()._asdict())

    status['loadavg'] = psutil.getloadavg()

    if max(status['loadavg']) > 10:
        issues.append("high load avg: %s" % repr(status['loadavg']))

    status['disk_usage'] = dict([(k+"_mb", v/1024/1024) if k != "percent" else (k, v)
                                 for k, v in dict(psutil.disk_usage(".")._asdict()).items()])

    with wfstore._lock:
        status['async'] = dict(qsize=async_queue.qsize(),
                               async_workflows_n=len(wfstore.async_workflows))

    #status['processes'] = processes

    return status, issues


@blprint.route('/test')
def test():
    results = {}
    expecting = []

    #TODO: use generalized testing
    with wfstore._lock:
        for template_nba in wfstore.notebook_adapters.values():
            if template_nba.name.startswith('test_'):
                key = template_nba.name

                if key in wfstore.async_workflows:
                    print("found", wfstore.async_workflows[key])

                    if isinstance(wfstore.async_workflows[key], dict):
                        print("found result seems a reasonable dict")
                        workflow_status = wfstore.async_workflows[key].get(
                            'workflow_status', 'done')
                        print("workflow_status", workflow_status)
                    else:
                        workflow_status = wfstore.async_workflows[key]

                    if workflow_status == 'done':
                        # and output notebook
                        results[template_nba.name] = wfstore.async_workflows[key]['exceptions']
                        print("workflow_status is done, results exceptions:",
                            results[template_nba.name])
                    else:
                        expecting.append(
                            dict(key=key, workflow_status=workflow_status))
                else:
                    async_task = AsyncWorkflow(key=key, target=template_nba.name, params=dict(
                        request_parameters=dict(location=os.path.dirname(template_nba.notebook_fn))))

                    async_queue.put(async_task)

                    wfstore.async_workflows[key] = 'started'
                    expecting.append(dict(key=key, workflow_status='submitted'))

    if expecting != []:
        return make_response(jsonify(dict(expecting=expecting)), 201)
    else:
        if all([v in ['[]', []] for v in results.values()]):
            print("tests passed")
            return make_response('all is OK: '+"; ".join(results.keys()), 200)
        else:
            print("tests NOT passed")
            return make_response(jsonify(results), 500)


@blprint.route('/')
def root():
    issues = []

    if len(issues) == 0:
        return {
                    "message": "all is ok!",
                    "version": os.getenv("ODA_WORKFLOW_VERSION"),
                    "last_author": os.getenv("ODA_WORKFLOW_LAST_AUTHOR"),
                    "last_changed": os.getenv("ODA_WORKFLOW_LAST_CHANGED")                    
            }
    else:
        return make_response(jsonify(issues=issues), 500)


@blprint.route('/status')
def status():
    with wfstore._lock:
        return jsonify(
            version=os.environ.get('WORKFLOW_VERSION', 'unknown'),
            started_at=current_app.config['STARTED_AT'].strftime("%s"),
            started_since=(datetime.datetime.now()-current_app.config['STARTED_AT']).seconds,
            background_jobs=len([w for w in wfstore.async_workflows if w]),
            stored_jobs=len(wfstore.async_workflows),
        )

@blprint.route('/async/clear')
def async_clear():
    with wfstore._lock:
        s = wfstore.async_workflows
        wfstore.async_reset()
        return jsonify(s)


@blprint.route('/async/size')
def async_size():
    with wfstore._lock:
        return jsonify({'async_size': len(wfstore.async_workflows)})


@blprint.route('/async/list')
def async_list():
    with wfstore._lock:
        return jsonify(wfstore.async_workflows)


@blprint.route('/async/qsize')
def async_qsize():
    return jsonify(dict(async_qsize=async_queue.qsize()))


def get_trace_list(since=None):
    r = []
    for d in glob.glob(os.path.join(tempfile.gettempdir(), "nb2w-*")):
        if since is not None and time.time() - os.stat(d).st_mtime > since:
            continue

        try:
            summary = yaml.load(
                open(os.path.join(d, "summary.yaml")), Loader=yaml.Loader)
        except Exception as e:
            summary = "unable to load: "+repr(e)

        r.append(
            dict(
                fn=d,
                mtime=os.stat(d).st_mtime,
                ctime=os.stat(d).st_ctime,
                summary=summary,
            )
        )

    return sorted(r, key=lambda x: ['ctime'])


@blprint.route('/trace/size')
def trace_size():
    return jsonify({"trace_size": len(glob.glob(os.path.join(tempfile.gettempdir(), "nb2w-*")))})


@blprint.route('/trace/list')
def trace_list():
    f = request.args.get('format', 'json')

    if f == 'html':
        return "not implemented"
    else:
        return jsonify(get_trace_list())


@blprint.route('/trace/<string:job>')
def trace_get(job):
    r = []

    r = []
    for fn in glob.glob(os.path.join(tempfile.gettempdir(), job, "*_output.ipynb")):
        r.append(fn)

    return jsonify(r)


@blprint.route('/trace/<string:job>/<string:func>')
def trace_get_func(job, func):
    if func == "custom.css":
        return ""

    include_glued_output = request.args.get('include_glued_output', True) == 'True'

    from nbconvert.exporters import HTMLExporter
    exporter = HTMLExporter()

    fn = os.path.join(tempfile.gettempdir(), job, func+"_output.ipynb")

    output, resources = exporter.from_filename(fn)

    if not include_glued_output:
        logger.info("include_glued_output arg passed")
        soup = BeautifulSoup(output, 'html.parser')
        div_glued_output = soup.find('div', {'class': 'celltag_injected-gather-outputs'})
        if div_glued_output is not None:
            div_glued_output.decompose()
        output = str(soup)
        logger.info("div element removed form html")

    return output


@blprint.route('/clear-cache')
def clear_cache():
    n_entries = None
    try:
        n_entries = len(cache.cache._cache)
    except Exception as e:
        pass

    cache.clear()

    if n_entries is not None:
        return 'cleared %i entries' % n_entries
    else:
        return 'cleared some entries'

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('--host', metavar='host',
                        type=str, default="127.0.0.1")
    parser.add_argument('--port', metavar='port', type=int, default=9191)
    parser.add_argument('--async-workers', metavar='N', type=int, default=3)
    #parser.add_argument('--tmpdir', metavar='tmpdir', type=str, default=None)
    parser.add_argument('--publish', metavar='upstream-url',
                        type=str, default=None)
    parser.add_argument(
        '--publish-as', metavar='published url', type=str, default=None)
    parser.add_argument(
        '--profile', metavar='service profile', type=str, default="oda")
    parser.add_argument('--debug', action="store_true")
    parser.add_argument('--one-shot', metavar='workflow', type=str)
    parser.add_argument('--pattern', type=str, default=r'.*')

    args = parser.parse_args()

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    root = logging.getLogger()

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    if args.debug:
        root.setLevel(logging.DEBUG)
        handler.setLevel(logging.DEBUG)
    else:
        root.setLevel(logging.INFO)
        handler.setLevel(logging.INFO)

    with wfstore._lock:
        wfstore.notebook_adapters = find_notebooks(args.notebook, pattern=args.pattern)
        wfstore.service_semantic_signature = ontology.service_semantic_signature(
            wfstore.notebook_adapters)
    
        app = create_app()
        
        if args.publish:
            logger.info("publishing to %s", args.publish)

            if args.publish_as:
                s = args.publish_as.split(":")
                publish_host, publish_port = ":".join(s[:-1]), int(s[-1])
            else:
                publish_host, publish_port = args.host, args.port

            for nba_name, nba in wfstore.notebook_adapters.items():
                publish.publish(args.publish, nba_name, publish_host, publish_port)

    thread_id = threading.get_ident()
    process_id = os.getpid()
    logger.info(f'service main thread id: {thread_id} ; process id: {process_id}')

  #  for rule in app.url_map.iter_rules():
 #       logger.debug("==>> %s %s %s %s",rule,rule.endpoint,rule.__class__,rule.__dict__)

    for worker_i in range(args.async_workers):
        async_worker = AsyncWorker('default-%i' % worker_i)
        async_worker.start()

    app.run(host=args.host, port=args.port)




if __name__ == '__main__':
    main()
