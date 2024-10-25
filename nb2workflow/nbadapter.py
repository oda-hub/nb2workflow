from __future__ import annotations

import ast

from dataclasses import dataclass, asdict
from functools import lru_cache, cached_property
import hashlib
import os
import glob
import shutil
from tokenize import generate_tokens, COMMENT
from typing import * # type: ignore 
# need wildcard import to resolve (semi-)arbitrary ForwardRef of annotations in nb
import yaml 
import re
import time
import tempfile
import subprocess
import yaml
import argparse
import json
import base64
import rdflib
import copy
import validators
import requests
import random
import string
import io
import threading

import papermill as pm
import scrapbook as sb
from typeguard import check_type, ForwardRefPolicy, TypeCheckError
import nbformat
from nbconvert import HTMLExporter
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from . import logstash

from oda_api.ontology_helper import Ontology, xsd_type_to_python_type

from nb2workflow.sentry import sentry
from nb2workflow.health import current_health
from nb2workflow.logging_setup import setup_logging
from nb2workflow.json import CustomJSONEncoder
from nb2workflow.helpers import is_mmoda_url, serialize_workflow_exception
from nb2workflow.semantics import understand_comment_references

from nb2workflow.semantics import understand_comment_references
from git import Repo, InvalidGitRepositoryError, GitCommandError

import logging
from threading import Lock

logger=logging.getLogger(__name__)

logstasher = logstash.LogStasher()

# TODO: will be configurable
oda_ontology_path = "http://odahub.io/ontology/ontology.ttl"
#oda_ontology_path = "/home/dsavchenko/Projects/MMODA/ontology/ontology.ttl"

class ModOntology(Ontology):
    def __init__(self, ontology_path):
        super().__init__(ontology_path)
        self.lock = Lock()
        self._is_ontology_available = True

    def get_datatype_restriction(self, param_uri):
        self.lock.acquire()
        dt = super()._get_datatype_restriction(param_uri)
        self.lock.release()
        if dt is None:
            logger.warning(f'Unknown datatype for owl_uri {param_uri}')
        return dt

    @property
    def is_ontology_available(self):
        # TODO will be developed properly in the ontology_helper
        return self._is_ontology_available

ontology = ModOntology(oda_ontology_path)
oda_prefix = str([x[1] for x in ontology.g.namespaces() if x[0] == 'oda'][0])

def run(notebook_fn, params: dict):
    nba = NotebookAdapter(notebook_fn)
    nba.execute(
        params,
        log_output=True,
        progress_bar=False
    )
    validate_oda_dispatcher(nba)
    return nba.extract_output()

class PapermillWorkflowIncomplete(Exception):
    pass

def cast_parameter(x,par):
    logger.debug("cast %s %s",x,par)
    if par['python_type'] is bool:
        if x in ['false', 'False', 0, '0', '']:
            return False
        elif x in ['true', 'True', 1, '1']:
            return True
        else:
            raise ValueError(f'Parameter {par["name"]} value "{x}" can not be interpreted as boolean.')
    if par['python_type'] in [list, dict]:
        try:
            if type(x) is str:
                decoded = json.loads(x)
            else:
                decoded = x
            if type(decoded) is par['python_type']:
                return decoded
            else:
                raise ValueError
        except:
            raise ValueError(f'Parameter {par["name"]} value "{x}" can not be interpreted as {par["python_type"].__name__}.')
    if x == '\x00':
        if par.get('is_optional', False):
            return None
        else:
            raise ValueError(f'Non-optional parameter is set to None')
    return par['python_type'](x)


def odahub_type_for_python_type(python_type: type):
    out_type = python_type.__name__

    xml_scheme_url = "http://www.w3.org/2001/XMLSchema#"

    if python_type == int:
        out_type = 'Integer'
        url_prefix = oda_prefix
    elif python_type == str:
        out_type = 'String'
        url_prefix = oda_prefix
    elif python_type == bool:
        out_type = 'Boolean'
        url_prefix = oda_prefix
    elif python_type == float:
        out_type = 'Float'
        url_prefix = oda_prefix
    else:
        url_prefix = xml_scheme_url

    output_url = f"{url_prefix}{out_type}"

    return output_url


def owl_type_for_python_type(python_type: type):
    out_type = python_type.__name__

    xml_scheme_url = "http://www.w3.org/2001/XMLSchema#"

    if python_type == int:
        out_type = 'integer'
        url_prefix = xml_scheme_url
    elif python_type == str:
        out_type = 'string'
        url_prefix = xml_scheme_url
    elif python_type == bool:
        out_type = 'boolean'
        url_prefix = xml_scheme_url
    elif python_type == float:
        out_type = 'float'
        url_prefix = xml_scheme_url
    else:
        url_prefix = oda_prefix

    output_url = f"{url_prefix}{out_type}"

    return output_url

T = TypeVar("T")
def reconcile_python_type(value: Any, 
                          type_annotation: str | type[T] | None = None, 
                          owl_type: str | None = None, 
                          extra_ttl: str | None = None, 
                          name: str = '') -> tuple[type, bool]:
    '''
    Reconcile python type of the default value with type and owl annotations
    We expect ~json here, so basically int, float, str, list, dict or None
    Respects duck typing: if default is int and float is allowed, returns float
    '''

    if type_annotation is None and owl_type is None:
        if value is not None:
            return type(value), False
        else:
            raise TypeCheckError(f"Default value of the required parameter {name} isn't defined.")

    owl_dt = None
    is_optional_owl = False
    if owl_type is not None:
        if extra_ttl is None: 
            extra_ttl = ''
        ontology.parse_extra_triples(extra_ttl, parse_oda_annotations=False)
        xsd_dt = ontology.get_datatype_restriction(owl_type)
        if xsd_dt:
            owl_dt = xsd_type_to_python_type(xsd_dt)
        is_optional_owl = ontology.is_optional(owl_type)

    is_optional_hint = False
    hint_fref = None
    if type_annotation:
        hint_fref = ForwardRef(type_annotation) if isinstance(type_annotation, str) else type_annotation
        
        try:
            check_type(None, hint_fref, forward_ref_policy=ForwardRefPolicy.ERROR)
        except TypeCheckError:
            is_optional_hint = False
        except NameError:
            raise TypeCheckError(f"Type hint {type_annotation} for parameter {name} can't be resolved.")
        else:
            is_optional_hint = True

    def check_type_both(v, fail_both_none=False):
        if fail_both_none and owl_dt is None and hint_fref is None:
            raise TypeCheckError('Type undefined')
        if owl_dt is not None:
            check_type(v, owl_dt)
        if hint_fref is not None:
            check_type(v, hint_fref) # forwardref is already checked for validity, no need to set policy

    # need special treatment for None because it may be allowed by one annotation type only. 
    # So other will fail checking value, but it's OK.
    if value is None:
        if not is_optional_owl and not is_optional_hint:
            raise TypeCheckError(f"Required parameter {name} shouldn't be None.")
        elif owl_dt is None and hint_fref is None:
            raise TypeCheckError(f"Default value of the parameter {name} can't be defined.")
        else:
            possible_types_examples = [1.1, 1, True, 'foo', [], {}]
            for ex in possible_types_examples: 
                try:
                    check_type_both(ex)
                except TypeCheckError:
                    pass
                else:
                    return type(ex), True
            raise TypeCheckError(f"No possible type is found for the parameter {name}.")
    elif isinstance(value, int) and not isinstance(value, bool):
        # be permissive if float is possible
        try:
            check_type_both(float(value), fail_both_none=True)
        except TypeCheckError:
            pass
        else:
            return float, is_optional_owl or is_optional_hint
        
        check_type_both(value)
        return int, is_optional_owl or is_optional_hint
    elif isinstance(value, bool):
        check_type_both(value)
        try:
            check_type_both(int(value), fail_both_none=True)
        except TypeCheckError:
            pass
        else:
            raise TypeCheckError(f"Boolean parameter {name} is annotated as integer.")
        return type(value), is_optional_owl or is_optional_hint
    else:
        check_type_both(value)
        return type(value), is_optional_owl or is_optional_hint



@dataclass
class InputParameter:
    raw_line: str
    name: str
    default_value: Any
    python_type: type
    comment: str
    owl_type: Optional[str] = None
    extra_ttl: Optional[str] = None
    is_optional: bool = False

    def as_dict(self):
        return asdict(self)
        

class NotebookAdapter:
    limit_output_attachment_file = None


    def __init__(self, notebook_fn, tempdir_cache=None, n_download_max_tries=10, download_retry_sleep_s=.5, max_download_size=500e6):
        self.notebook_fn = os.path.abspath(notebook_fn)
        self.name = notebook_short_name(notebook_fn)
        self.tempdir_cache = tempdir_cache
        self._graph = rdflib.Graph()
        logger.debug("notebook adapter for %s", self.notebook_fn)
        logger.debug(self.extract_parameters())
        self.n_download_max_tries = n_download_max_tries
        self.download_retry_sleep_s = download_retry_sleep_s
        self.max_download_size = max_download_size

    @property
    def graph(self):
        # need to populate the graph (currently only notebook-wide annotations)
        self.extract_parameters()
        return self._graph

    @staticmethod
    def get_unique_filename_from_url(file_url):
        parsed_arg_par_value = urlparse(file_url)
        file_name_prefix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        file_name = f"{file_name_prefix}_{parsed_arg_par_value.path.split('/')[-1]}"
        return file_name

    def new_tmpdir(self, cache_key=None):
        logger.debug("tmpdir was "+getattr(self,'_tmpdir','unset'))
        self._tmpdir = None
        logger.debug("tmpdir became %s", self._tmpdir)

        newdir = self.tmpdir
        if ( self.tempdir_cache is not None ) and ( cache_key is not None ):
            self.tempdir_cache[cache_key] = newdir

        return newdir

    @property
    def tmpdir(self):
        if getattr(self,'_tmpdir', None) is None:
            self._tmpdir = tempfile.mkdtemp(prefix="nb2w-")
        if self._tmpdir is None:
            raise RuntimeError("can no create tempdif")
        return self._tmpdir
    
    @property
    def preproc_notebook_fn(self):
        return os.path.join(self.tmpdir, os.path.basename(self.notebook_fn.replace(".ipynb","_preproc.ipynb")))

    @property
    def output_notebook_fn(self):
        return os.path.join(self.tmpdir, os.path.basename(self.notebook_fn.replace(".ipynb","_output.ipynb")))

    def read(self):
        if not os.path.exists(self.notebook_fn):
            raise RuntimeError(f"notebook {self.notebook_fn} not found in {os.getcwd()}")

        return nbformat.reads(open(self.notebook_fn).read(), as_version=4)

    _notebook_origin = None

    @property
    def notebook_origin(self):
        if self._notebook_origin is None:
            notebook_dir = os.path.dirname(self.notebook_fn)
            logger.info('notebook_dir: %s', notebook_dir)
            try:
                url = subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=notebook_dir).decode().strip()
                revision = subprocess.check_output(["git", "describe", "--always", "--tags"], cwd=notebook_dir).decode().strip()
                self._notebook_origin = f"{url}#{revision}"
            except subprocess.CalledProcessError:
                logger.warning('Not a git repo, making local name.')
                self._notebook_origin = f'file://{os.path.abspath(notebook_dir)}'

        return self._notebook_origin

    @property
    def unique_name(self):
        return f"{self.name}_{hashlib.md5(self.notebook_origin.encode()).hexdigest()[:8]}"

    def export_html(self, fn=None):
        if fn is None:
            fn = "{}_output.html".format(self.name)

        if False:
            html_exporter = HTMLExporter()
            html_exporter.template_file = 'basic'
            (body, resources) = html_exporter.from_notebook_node(self.read())
            open(fn, "w").write(body)
        else:
            logging.info("converting... {}".format(subprocess.check_call(["jupyter", "nbconvert", "--to", "html", self.output_notebook_fn, '--output', os.path.abspath(fn)])))

        logger.info("exported html to %s", fn)

        return fn

    @property
    def nb_uri(self):
        return rdflib.URIRef(f"{oda_prefix}{self.unique_name}")      
    
    @staticmethod
    def _pop_comment_by_line(comment_tokens, l):
        for i, x in enumerate(comment_tokens):
            if x.start[0]==l:
                res = comment_tokens.pop(i)
                return res.string[1:]
        return ''
    
    def parse_source_multiline(self, source: str) -> dict[str, list[dict]]:
        result = {'assign': [], 
                  'standalone': []}
        
        tokens = generate_tokens(io.StringIO(source).readline)
        comments = []
        for token in tokens:
            if token.type == COMMENT:
                comments.append(token)
           
        parsed = ast.parse(source)
        for node in parsed.body:
            node_code = "\n".join(source.split('\n')[node.lineno-1:node.end_lineno])
            if isinstance(node, ast.Assign):
                if len(node.targets) != 1:
                    raise NotImplementedError(f'Multiple assignment is not supported:\n{node_code}')
                varname = node.targets[0].id
                type_annotation = None
                value_node = node.value
            elif isinstance(node, ast.AnnAssign):
                varname = node.target.id
                type_annotation = ast.unparse(node.annotation)
                value_node = node.value
            elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Name):
                # "hanging" output declaration
                value_node = None
                type_annotation = None
                varname = node.value.id
            else:
                logger.info(f"Skipping {node}")
                continue
            
            if value_node is not None:
                try:
                    value = ast.literal_eval(value_node)
                except ValueError:
                    value = ast.unparse(value_node)
            else:
                value = None
            
            comment = ''
            for line in range(node.lineno, node.end_lineno+1):
                comment = self._pop_comment_by_line(comments, line)
                # annotation must appear right after definition
                if line != node.end_lineno:
                    continue
            
            result['assign'].append(dict(varname = varname, 
                                         type_annotation = type_annotation, 
                                         value = value, 
                                         comment = comment,
                                         raw_line = node_code))
            
        # now parse full-line comments
        for comment in comments:
            cstring = comment.string[1:]
            result['standalone'].append(cstring)        
        
        return result
    
    def extract_parameters_from_cell(self, cell):
        parameters = {}
        
        parsed_cell = self.parse_source_multiline(cell['source'])
        for par_detail in parsed_cell['assign']:
            
            # May need to have fallback type to properly parse owl
            if par_detail['value'] is not None: 
                fallback_type = odahub_type_for_python_type(type(par_detail['value']))
            else:
                try:
                    fallback_type = reconcile_python_type(None, 
                                        type_annotation=par_detail['type_annotation'],
                                        name = par_detail['varname'])[0]
                    fallback_type = odahub_type_for_python_type(fallback_type)
                except TypeCheckError:
                    fallback_type = None
            
            # Now full recoincilation
            parsed_comment = understand_comment_references(par_detail['comment'],
                                                           fallback_type=fallback_type)
            
            python_type, is_optional = reconcile_python_type(par_detail['value'],
                                            type_annotation=par_detail['type_annotation'],
                                            owl_type=parsed_comment.get('owl_type', None),
                                            extra_ttl=parsed_comment.get('extra_ttl', None),
                                            name = par_detail['varname'])
            
            par = InputParameter(raw_line = par_detail['raw_line'],
                                 name = par_detail['varname'],
                                 default_value = par_detail['value'],
                                 python_type = python_type,
                                 comment = par_detail['comment'],
                                 owl_type = parsed_comment.get('owl_type', None),
                                 extra_ttl = parsed_comment.get('extra_ttl', None),
                                 is_optional=is_optional)
            
            # This leads to some recursion, but it's not really used anywhere. 
            # TODO: integrate with ontology.function_semantic_signature
            # if par.extra_ttl is not None:
            #     self.graph.parse(data=par.extra_ttl)
            parameters[par.name] = par.as_dict()
            parameters[par.name]['value'] = par.as_dict()['default_value']
        

        for cstring in parsed_cell['standalone']:
            p = understand_comment_references(cstring, base_uri=self.nb_uri)
            if p is not None:
                try:
                    self._graph.parse(data=p['extra_ttl'])
                except Exception as e:
                    logger.warning("not a turtle: %s", p['extra_ttl'])

        return parameters

    @lru_cache
    def extract_parameters(self):
        nb = self.read()

        self.input_parameters = {}
        self.system_parameters = {}
               
        for cell in nb.cells:
            for tag, attr in [
                    ('parameters', 'input_parameters'),
                    ('system-parameters', 'system_parameters'),
                    ('injected-parameters', 'input_parameters'),
                    ]:
                if tag in cell.metadata.get('tags', []):
                    pars = self.extract_parameters_from_cell(cell)
                    pars = {**getattr(self, attr), **pars}
                    setattr(self, attr, pars)

        return self.input_parameters

    @cached_property
    def token_access(self):
        oda_token_access = rdflib.URIRef(f"{oda_prefix}oda_token_access")
        _token_access = None
        for s, p, o in self.graph.triples((None, oda_token_access, None)):
            if _token_access is not None:
                raise RuntimeError('Multiple oda_token_access annotations')
            _token_access = o
        return _token_access
        
    @cached_property
    def extra_ttl(self) -> str:
        return self.graph.serialize(format='turtle')
    
    def interpret_parameters(self,parameters):
        expected_parameters = self.extract_parameters()
        request_parameters = dict()

        unexpected_parameters = []
        issues=[]

        for arg in parameters:
            if arg.startswith("_"): continue

            logger.info("request arg %s",parameters[arg])
            if arg in expected_parameters:
                try:
                    request_parameters[arg] = cast_parameter(parameters.get(arg),expected_parameters.get(arg))
                    logger.info("request arg %s provided as %s",parameters[arg],request_parameters[arg])
                except ValueError as e:
                    issues.append(e.args[0])
            else:
                unexpected_parameters.append(arg)

        if len(unexpected_parameters) > 0:
            issues += [f'found unexpected request parameters: {", ".join(unexpected_parameters)}, can be {", ".join(expected_parameters.keys())}']
            
        return dict(
                    issues=issues,
                    request_parameters=request_parameters,
                )

    def update_summary(self, **d):
        if not hasattr(self, '_summary'):
            self._summary = dict(
                                name=self.name,
                                initialized=dict(s_epoch=time.time(), isot=time.strftime("%Y-%m-%d %H:%M:%S")),
                            )

        state=d.pop('state', None)

        self._summary.update(d)
        
        if state is not None:
            self._summary['state'] = self._summary.get("state", []) + [(time.time(), state)]

        fn = os.path.join(self.tmpdir, "summary.yaml")
        yaml.dump(self._summary, open(fn, "w"))

        

    def execute(self, parameters, progress_bar=True, log_output=True, inplace=False, tmpdir_key=None, context=None):

        if context is None:
            context = {}
        t0 = time.time()
        logstasher.log(dict(origin="nb2workflow.execute", event="starting", parameters=parameters, workflow_name=notebook_short_name(self.notebook_fn), health=current_health()))

        logger.info("starting job")
        exceptions = self._execute(parameters, progress_bar, log_output, inplace, context=context, tmpdir_key=tmpdir_key)

        tspent = time.time() - t0
        logstasher.log(dict(origin="nb2workflow.execute",
                            event="done",
                            parameters=parameters,
                            workflow_name=notebook_short_name(self.notebook_fn),
                            exceptions=list(map(serialize_workflow_exception, exceptions)),
                            health=current_health(),
                            time_spent=tspent))

        return exceptions

    def _execute(self, parameters, progress_bar=True, log_output=True, inplace=False, context={}, tmpdir_key=None):

        if not inplace :
            tmpdir = self.new_tmpdir(tmpdir_key)
            logger.info("new tmpdir: %s", tmpdir)
            repo_dir = os.path.dirname(os.path.realpath(self.notebook_fn))
            try:
                repo = Repo(repo_dir)
                repo.clone(tmpdir, multi_options=["--recurse-submodules"])
            except InvalidGitRepositoryError:
                logger.warning(f"repository {repo_dir} is invalid, will attempt copytree")
                os.rmdir(tmpdir)
                shutil.copytree(os.path.dirname(os.path.realpath(self.notebook_fn)), tmpdir)
            except GitCommandError as e:
                logger.warning(f"git command error: {e}")
                if 'git-lfs' in str(e):
                    # this error may occur if the repo was originally cloned by the different version of git utility
                    # e.g. when repo is mounted with docker run -v
                    raise Exception("We got some problem cloning the repository, the problem seems to be related to git-lfs. You might want to try reinitializing git-lfs with 'git-lfs install; git-lfs pull'")
                else:
                    raise e
        else:
            tmpdir =os.path.dirname(os.path.realpath(self.notebook_fn))
            logger.info("executing inplace, no tmpdir is input dir: %s", tmpdir)

        r = self.handle_url_params(parameters, tmpdir, context=context)

        if len(context) > 0:
            self._pass_context(tmpdir, context)

        self.update_summary(state="started", parameters=parameters)

        self.inject_output_gathering()
        exceptions = []

        if len(r['exceptions']) > 0:
            exceptions.extend(r['exceptions'])
        else:
            ntries = 10
            while ntries > 0:
                try:
                    thread_id = threading.get_ident()
                    process_id = os.getpid()
                    logger.info(f'pm.execute_notebook thread id: {thread_id} ; process id: {process_id}')

                    pm.execute_notebook(
                       self.preproc_notebook_fn,
                       self.output_notebook_fn,
                       parameters = r['adapted_parameters'],
                       progress_bar = False,
                       log_output = True,
                       cwd = tmpdir,
                    )
                except pm.PapermillExecutionError as e:
                    exceptions.append([e,e.args])
                    logger.info(e)
                    logger.info(e.args)

                    if e.ename == "WorkflowIncomplete":
                        logger.info("detected incomplete workflow")
                        self.update_summary(state="incomplete dependency", dependency=repr(e))
                        raise  PapermillWorkflowIncomplete()

                except nbformat.reader.NotJSONError:
                    ntries -= 1
                    logger.info("retrying... %s", ntries)
                    time.sleep(2)
                    continue

                break

        if len(exceptions) == 0:
            self.update_summary(state="done")
        else:
            self.update_summary(state="failed", exceptions=list(map(serialize_workflow_exception, exceptions)))

        return exceptions

    def _pass_context(self, workdir: str, context: dict):
        """
        save context to file .oda_api_context in the notebook dir where it can be accessed by ODA API
        :param workdir: directory to save notebook in
        """
        from oda_api import context_file

        if str(self.token_access).endswith('InOdaContext'):
            if 'token' not in context:
                raise RuntimeError('token is not provided')
        elif 'token' in context:
            # don't pass token since it was not reqested
            context = context.copy()
            del context['token']

        context_file_path = os.path.join(workdir, context_file)
        with open(context_file_path, 'wt') as output:
            json.dump(context, output)
        logger.info("context file created: %s", context_file_path)

    def extract_pm_output(self):
        nb = sb.read_notebook(self.output_notebook_fn)

        outputs=dict()
        for i, d in nb.scraps.dataframe.iterrows():
            logger.debug("d... %s",d)
            #if d.dtype == "record":
            outputs[d['name']]=d['data']

        return outputs

    @lru_cache
    def extract_output_declarations(self):
        nb=self.read()

        outputs = {}

        for cell in nb.cells:
            if 'outputs' in cell.metadata.get('tags',[]):
                parsed_cell = self.parse_source_multiline(cell['source'])
                
                # TODO: may use annotations (type/ontology) to get python type
                for outp_detail in parsed_cell['assign']:
                    parsed_comment = understand_comment_references(outp_detail['comment'])
                    outputs[outp_detail['varname']] = {
                        'name': outp_detail['varname'],
                        'value': outp_detail['value'],
                        'python_type': str, # NOTE: kept for backward compatibility
                        'comment': outp_detail['comment'],
                        'owl_type': parsed_comment.get('owl_type', None),
                        'extra_ttl': parsed_comment.get('extra_ttl', None),
                    }

        return outputs 

    def extract_output(self):
        return self.extract_pm_output()

    def download_file(self, file_url, tmpdir):
        n_download_tries_left = self.n_download_max_tries
        size_ok = False
        file_downloaded = False
        file_name = NotebookAdapter.get_unique_filename_from_url(file_url)
        file_path = os.path.join(tmpdir, file_name)
        for _ in range(n_download_tries_left):
            step = 'getting the file size'
            reason = 'connection error'
            if not size_ok:
                try:
                    response = requests.head(file_url, allow_redirects=True)
                except requests.exceptions.ConnectionError as ce:
                    logger.warning(
                        (f"An issue, due to {reason}, occurred when attempting to {step} of the file at the url {file_url}. "
                         f"Sleeping {self.download_retry_sleep_s} seconds until retry")
                    )
                    time.sleep(self.download_retry_sleep_s)
                    continue
                reason = 'invalid status code'
                if response.status_code == 200:
                    file_size = int(response.headers.get('Content-Length', 0))
                    if file_size > self.max_download_size:
                        msg = ("The file appears to be too large to download, "
                               f"and the download limit is set to {self.max_download_size} bytes.")
                        logger.warning(msg)
                        sentry.capture_message(msg)
                        raise Exception(msg)
                else:
                    logger.warning(
                        (f"An issue, due to {reason}, occurred when attempting to {step} of the file at the url {file_url}. "
                         f"Sleeping {self.download_retry_sleep_s} seconds until retry")
                    )
                    time.sleep(self.download_retry_sleep_s)
                    continue
            size_ok = True
            step = 'downloading file'
            reason = 'connection error'
            try:
                response = requests.get(file_url)
            except requests.ConnectionError as ce:
                logger.warning(
                    (f"An issue, due to {reason}, occurred when attempting to {step} of the file at the url {file_url}. "
                     f"Sleeping {self.download_retry_sleep_s} seconds until retry")
                )
                time.sleep(self.download_retry_sleep_s)
                continue
            reason = 'invalid status code'
            if response.status_code == 200:
                with open(file_path, 'wb') as file:
                    file.write(response.content)
                file_downloaded = True
                break
            else:
                logger.warning(
                    (f"An issue occurred when attempting to {step} the file at the url {file_url}. "
                     f"Sleeping {self.download_retry_sleep_s} seconds until retry")
                )
                time.sleep(self.download_retry_sleep_s)
                continue

        if not (file_downloaded and size_ok):
            msg = (f"An issue, due to {reason}, occurred when attempting to {step} at the url {file_url}. "
                   "This might be related to an invalid url, please check the input provided")
            logger.warning(msg)
            sentry.capture_message(msg)
            raise Exception(msg)

        return file_name

    def handle_url_params(self, parameters, tmpdir, context={}):
        adapted_parameters = copy.deepcopy(parameters)
        exceptions = []
        posix_path_with_annotations_pattern = re.compile(rf"^{re.escape(oda_prefix)}.*_POSIXPath_")
        for input_par_name, input_par_obj in self.input_parameters.items():
            if ontology.is_ontology_available:
                parameter_hierarchy = ontology.get_parameter_hierarchy(input_par_obj['owl_type'])
                is_posix_path = f"{oda_prefix}POSIXPath" in parameter_hierarchy
            else:
                is_posix_path = f"{oda_prefix}POSIXPath" == input_par_obj['owl_type'] or \
                                posix_path_with_annotations_pattern.match(input_par_obj['owl_type']) is not None
            if is_posix_path:
                arg_par_value = parameters.get(input_par_name, None)
                if arg_par_value is None:
                    arg_par_value = input_par_obj['default_value']
                if validators.url(arg_par_value, simple_host=True):
                    logger.info(f"checking url: {arg_par_value}")
                    if is_mmoda_url(arg_par_value):
                        logger.debug(f"{arg_par_value} is an mmoda url")
                        token = context.get('token', None)
                        if token is not None:
                            logger.debug(f'adding token to the url: {arg_par_value}')
                            url_parts = urlparse(adapted_parameters[input_par_name])
                            url_args = parse_qs(url_parts.query)
                            url_args['token'] = [token] # the values in the dictionary need to be lists
                            new_url_parts = url_parts._replace(query=urlencode(url_args, doseq=True))
                            adapted_parameters[input_par_name] = urlunparse(new_url_parts)
                            logger.debug(f"updated url: {adapted_parameters[input_par_name]}")
                            arg_par_value = adapted_parameters[input_par_name]

                    logger.debug(f'download {arg_par_value}')
                    try:
                        file_name = self.download_file(arg_par_value, tmpdir)
                        adapted_parameters[input_par_name] = file_name
                    except Exception as e:
                        exceptions.append(e)

        return dict(
            adapted_parameters=adapted_parameters,
            exceptions=exceptions
        )

    def inject_output_gathering(self):
        outputs = self.extract_output_declarations()

        output_gather_content="""
import papermill as pm
import scrapbook as sb
import base64
import json
import hashlib
import os
    
from nb2workflow.nbadapter import denumpyfy
from nb2workflow.json import CustomJSONEncoder

"""
        for output in outputs.keys():
            logger.debug("output: %s",output)
            output_gather_content+="""
try:
    sb.glue("{output}",denumpyfy({output}))
except Exception as e:
    print("failed to glue {output}", {output})
    print("will glue jsonified")
    sb.glue("{output}",json.dumps(denumpyfy({output}), cls=CustomJSONEncoder))
""".format(output=output)

            output_gather_content += f"""
if isinstance({output},str) and os.path.exists({output}):
    variable_name = "{output}"
    fn = {output}
    content = open(fn ,'rb').read()    

    if {self.limit_output_attachment_file} is None or len(content) < {self.limit_output_attachment_file}:
        encoded = base64.b64encode(content).decode()
        print("glueing file", fn)
        sb.glue(variable_name + "_content", encoded)
    else:
        # TODO: make a customizable upload to different DL platforms; before that it should be enabled with caution    
        nb2w_store_base = os.getenv("NB2W_CACHE", os.getenv("HOME") + "/.cache/nb2workflow/bigoutputs")
        os.makedirs(nb2w_store_base, exist_ok=True)
        url = "file://" + nb2w_store_base +  "/" + str(hashlib.md5(content).hexdigest())
        print("storing file to URL", url)
        with open(url.replace("file://", ""), "wb") as f:
            f.write(content)

        sb.glue(\"{output}_url\", url)
"""
            output_gather_content+="\n".format(output=output)

        nb = self.read()

        newcell = nbformat.v4.new_code_cell(source=output_gather_content)
        newcell.metadata['tags'] = ['injected-gather-outputs']

        nb = self.read()

        if nbformat.current_nbformat !=  nb.nbformat:
            logger.error("we assume nbformat version %s, but provided notebook is version %s, refusing!", 
                          nbformat.current_nbformat,
                          nb.nbformat)
            raise RuntimeError("incompatabile notebook major version")

        logger.info("provided notebook nbformat version minor %s while nbformat package minor version %s",
                    nb.nbformat_minor, nbformat.current_nbformat_minor)
                
        if nbformat.current_nbformat_minor == nb.nbformat_minor:
            logger.info("versions of notebook and environment match")
        elif nbformat.current_nbformat_minor < nb.nbformat_minor:
            logger.warn("notebook is newer than envionment package! please update your system or expect warnings")
        elif  nbformat.current_nbformat_minor > nb.nbformat_minor:
            logger.warning("will attempt to convert, but expect other warnings!")                            

            nb = nbformat.v4.convert.upgrade(nb, from_minor=nb.nbformat_minor)
        else:
            raise NotImplementedError


        nb.cells = nb.cells + [newcell] 

        logger.info("stored pre-processed notebook as %s", self.preproc_notebook_fn)
        pm.iorw.write_ipynb(nb, self.preproc_notebook_fn)

    def get_system_parameter_value(self, name, default):
        if name in self.system_parameters:
            return self.system_parameters.pop(name)['default_value'] 

        return default


    def remove_tmpdir(self):
        if self._tmpdir is not None:
            logger.info("removing tmpdir %s", self._tmpdir)
            shutil.rmtree(self._tmpdir)
        else:
            logger.info("no dir to remove")


def notebook_short_name(ipynb_fn):
    return os.path.basename(ipynb_fn).replace(".ipynb","")

def find_notebooks(source, tests=False, pattern = r'.*') -> Dict[str, NotebookAdapter]:

    def base_filter(fn): 
        good = "output" not in fn and "preproc" not in fn
        good = good and re.match(pattern, os.path.basename(fn)) 
        return good
        

    if tests:
        filt = lambda fn: base_filter(fn) and "/test_" in fn
    else:
        filt = lambda fn: base_filter(fn) and "/test_" not in fn

    if os.path.isdir(source):
        notebooks=[ fn for fn in glob.glob(source+"/*ipynb") if filt(fn) ]        

        logger.debug("found notebooks: %s",notebooks)

        if len(notebooks)==0:
            raise Exception("no notebooks found in the directory:",source)

        notebook_adapters=dict([
                (notebook_short_name(notebook),NotebookAdapter(notebook)) for notebook in notebooks
            ])
        logger.debug("notebook adapters: %s",notebook_adapters)


    elif os.path.isfile(source):
        if pattern != r'.*':
            logger.warning('Filename pattern is set but source %s is a single file. Ignoring pattern.')
        notebook_adapters={notebook_short_name(source): NotebookAdapter(source)}

    else:
        raise Exception("requested notebook not found:",source)

    return notebook_adapters

def nbinspect(nb_source, out=True, machine_readable=False):
    nbas = find_notebooks(nb_source)

    # class CustomEncoder(json.JSONEncoder):
    #     def default(self, obj):
    #         if isinstance(obj, type):
    #             return str(obj)
    #         return json.JSONEncoder.default(self, obj)

    summary = []

    for n, nba in nbas.items():
        summary.append({
                "parameters": nba.extract_parameters(),
                "outputs": nba.extract_output_declarations()
            })
        print(json.dumps(summary[-1], indent=4, sort_keys=True, cls=CustomJSONEncoder))

    if machine_readable:
        print("WORKFLOW-NB-SIGNATURE:", json.dumps(summary, cls=CustomJSONEncoder))
    

def nbreduce(nb_source, max_size_mb):
    cellsize_limit = None
    largest_cellsize = None


    while True:
        current_size_mb = os.path.getsize(nb_source)/1024./1024
        logging.info('notebook %s size %.4lg Mb', nb_source, current_size_mb)


        nb = nbformat.reads(open(nb_source).read(), as_version=4)

        newcells = []
        outputs_left = 0
        for i_cell, cell in enumerate(nb.cells):
            logging.info('try to reduce CELL #%i', i_cell)
    
            try:
                cellsize = len(json.dumps(cell.outputs))
            except AttributeError:
                logging.info("cell has no outputs, ignoring")
                continue

            if largest_cellsize is None or largest_cellsize < cellsize:
                largest_cellsize = cellsize

            logging.info("cell size %.5lg largest cell size %.5lg", cellsize, largest_cellsize)
            
            if 'injected-gather-outputs' in cell.metadata.get('tags', []):
                logging.info("is injected-gather-outputs: skipping")
                continue

            if cellsize_limit is not None and cellsize >= cellsize_limit:
                logging.info('cleaning cell')
                cell.outputs = []
                largest_cellsize = None


            if cell.outputs != []:
                logging.info('this cell has viable outputs')
                outputs_left += 1
        
            newcells.append(cell)

        if outputs_left == 0:
            logging.info('notebook size %.4lg Mb, and not more outputs left, cleaning aborted', current_size_mb)
            return
        else:
            logging.info('notebook size %.4lg Mb, still has %i outputs: cleaning may continue', current_size_mb,  outputs_left)

        nb.cells = newcells 
        pm.iorw.write_ipynb(nb, nb_source)

        if current_size_mb < max_size_mb:
            logging.info('notebook size %.4lg Mb is smaller than required %.5lg Mb, only cleaning gathering', current_size_mb, max_size_mb)
            return
        else:
            logging.info('notebook size %.4lg Mb is larger than required %.5lg Mb, setting cell size limit to the largest cell %s', 
                         current_size_mb, 
                         max_size_mb, 
                         cellsize_limit)

            cellsize_limit = largest_cellsize


def validate_oda_dispatcher(nba: NotebookAdapter, optional=True, machine_readable=False):
    logger.info('validating with ODA dispatcher plugin')

    try:
        from dispatcher_plugin_nb2workflow.queries import NB2WProductQuery
    except Exception as e:
        logger.warning("unable to import dispatcher_plugin_nb2workflow.queries.NB2WProductQuery: %s", e)
        if not optional:
            logger.error("dispatcher validation is not optional!")
            raise
    else:
        nbpq = NB2WProductQuery('testname',
                                'testproduct',
                                nba.extract_parameters(),
                                nba.extract_output_declarations(),
                                oda_ontology_path)

        output = nba.extract_output()

        logger.debug(json.dumps(output, indent=4))

        class MockRes:
            @staticmethod
            def json():
                return {
                    'data': {
                        'output': output
                    }
                }

        logger.debug("parameters as interpreted by dispatcher: %s", json.dumps(json.loads(nbpq.get_parameters_list_as_json()), indent=4))

        dispatcher_parameters = json.loads(nbpq.get_parameters_list_as_json())

        for parameter in dispatcher_parameters:
            logger.info("\033[32mODA dispatcher parameter \033[0m: %s", parameter)

        prod_list = nbpq.build_product_list(instrument=None, res=MockRes, out_dir=None)

        for prod in prod_list:
            logger.info("\033[33mworkflow the output produces ODA product \033[0m: \033[31m%s\033[0m (%s) %s", prod.name, prod.type_key, prod)

        if machine_readable:
            print("WORKFLOW-DISPATCHER-SIGNATURE:", json.dumps([
                        {"parameters": dispatcher_parameters,
                         "outputs": [{'name': prod.name, 'type': prod.type_key, 'class_name': prod.__class__.__name__} for prod in prod_list]
                        }]))
        
    

def nbrun(nb_source, inp, inplace=False, optional_dispather=True, machine_readable=False):

    nbas = find_notebooks(nb_source)

    if len(nbas) > 1:
        nba = nbas[inp.pop('notebook')]
    elif len(nbas) == 1:
        nba = list(nbas.values())[0]
    else:
        raise RuntimeError

    print("inp", inp)

    r = nba.interpret_parameters(inp)
    
    if r['issues'] != []:
        raise Exception(r['issues'])

    pars = r['request_parameters']

    logging.info("found parameters %s", repr(pars))

    exceptions = nba.execute(pars, inplace=inplace)

    if len(exceptions) == 0:
        logging.info("execution SUCCESSFUL!")
    else:
        logging.error("FAILED: %s", exceptions)

        with open("{}_exceptions.json".format(nba.name), "w") as f:
            json.dump(list(map(serialize_workflow_exception, exceptions)), f)

        fn = nba.export_html()
        open("{}_output.ipynb".format(nba.name), "wb").write(open(nba.output_notebook_fn, "rb").read())

        raise Exception(f"FAILED to execute {nba.name} {exceptions} html exported in {fn}")

    r={}
    for k,v in nba.extract_output().items():
        r[k.strip()]=repr(v)

    with open("{}_output.json".format(nba.name), "w") as f:
        json.dump(r, f)
    
    with open("cwl.output.json", "w") as f:
        json.dump(r, f)

    nbdata = open(nba.output_notebook_fn, "rb").read()
    nbfn = "{}_output.ipynb".format(nba.name)
    open(nbfn, "wb").write(nbdata)

    #TODO: store if not too big?   
    #r['output_notebook'] = nbfn
    #r['output_notebook_content'] = base64.b64encode(nbdata).decode()
        
    htmlfn = "{}_output.html".format(nba.name)
    nba.export_html(htmlfn)
    
    r['output_notebook_html'] = htmlfn
    r['output_notebook_html_content'] = base64.b64encode(open(htmlfn, "rb").read()).decode()

    validate_oda_dispatcher(nba, optional=optional_dispather, machine_readable=machine_readable)

    return r


def traverse_structure(structure, modifier):
    if isinstance(structure, dict):
        return { k:traverse_structure(v, modifier) for k,v in structure.items() }

    if isinstance(structure, list):
        return [ traverse_structure(v, modifier) for v in structure ]

    return modifier(structure)



def denumpyfy(data):
    import numpy as np

    def numpy_adapter(d):
        if isinstance(d, np.bool_):
            return bool(d)

        if isinstance(d, np.ndarray):
            return d.tolist()
        
        if isinstance(d, np.float16) or \
           isinstance(d, np.float32) or \
           isinstance(d, np.float64):
            return float(d)

        if isinstance(d, np.int16) or \
           isinstance(d, np.int32) or \
           isinstance(d, np.int64):
            return int(d)

        return d

    return traverse_structure(
                data,
                numpy_adapter,
           )

def main_reduce():
    parser = argparse.ArgumentParser(description='Reduce notebook size') 
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('maxsizeMb', metavar='max_size_mb', type=float)
    parser.add_argument('--debug', action="store_true")
    
    args = parser.parse_args()

    setup_logging(args.debug)

    nbreduce(args.notebook, args.maxsizeMb)

def main_inspect():
    parser = argparse.ArgumentParser(description='Inspect some notebooks') # run locally, remotely, semantically
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('--debug', action="store_true")
    parser.add_argument('--machine-readable', action="store_true")        
    
    args = parser.parse_args()

    setup_logging(args.debug)

    nbinspect(args.notebook, machine_readable=args.machine_readable)


def main():
    parser = argparse.ArgumentParser(description='Run some notebooks') # run locally, remotely, semantically
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('--debug', action="store_true")
    parser.add_argument('--inplace', action="store_true")
    parser.add_argument('--mmoda-validation', action="store_true")        
    parser.add_argument('--machine-readable', action="store_true")        
    
    parser.add_argument('inputs', nargs=argparse.REMAINDER)

    args = parser.parse_args()

    inputs={}
    for i in args.inputs:
        if not i.startswith("--inp-"): continue
        k,v = i.replace('--inp-','').split("=")
        inputs[k] = v
        
    setup_logging(args.debug)

    nbrun(args.notebook, inputs, inplace=args.inplace, optional_dispather=not args.mmoda_validation, machine_readable=args.machine_readable)


if __name__ == "__main__":
    main()
