from ast import literal_eval
import hashlib
import os
import sys
import glob
import shutil
from typing import Optional, Dict
import uuid
import yaml 
import re
import time
import tempfile
import pprint
import subprocess
import yaml
import argparse
import json
import base64
import rdflib

import papermill as pm
import scrapbook as sb
import nbformat
from nbconvert import HTMLExporter

from . import logstash

from nb2workflow.health import current_health
from nb2workflow import workflows
from nb2workflow.logging_setup import setup_logging
from nb2workflow.json import CustomJSONEncoder

from nb2workflow.semantics import understand_comment_references, oda_ontology_prefix

import logging

logger=logging.getLogger(__name__)

logstasher = logstash.LogStasher()


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
    return par['python_type'](x)



def parse_nbline(line: str, nb_uri=None) -> Optional[dict]:
    """
    this function is used in 3 cases:
    * input parameters
    * outputs
    * full-line comments - to annotate notebook itself
    """

    if line.strip() == "":
        return None

    elif line.strip().startswith("#"):
        comment = line.strip().strip("#")
        logger.debug("found detached comment: \"%s\"",line)

        if nb_uri is not None:
            return understand_comment_references(comment, nb_uri)
        else:
            return None

    else:
        if "#" in line:
            assignment_line, comment = line.split("#",1)
        else:
            assignment_line = line
            comment = ""
            
        if "=" in assignment_line:
            name, value_str = assignment_line.split("=", 1)
            name = name.strip()
            value_str = value_str.strip()
        else:
            name = assignment_line.strip()
            value_str = None

        try:
            value = literal_eval(value_str)
            python_type = type(value)
        except Exception:
            value = value_str
            python_type = str
            
        parsed_comment = understand_comment_references(comment, fallback_type=odahub_type_for_python_type(python_type))
        
        logger.info("parameter name=%s value=%s python_type=%s, owl_type=%s extra_ttl=%s", 
                    name, value, python_type, parsed_comment['owl_type'], parsed_comment['extra_ttl'])

        return dict(
                    name = name,
                    value = value,
                    python_type = python_type,
                    comment = comment,
                    owl_type = parsed_comment.get('owl_type', None),
                    extra_ttl = parsed_comment.get('extra_ttl', None),
                )


def odahub_type_for_python_type(python_type: type):
    out_type = python_type.__name__

    xml_scheme_url = "http://www.w3.org/2001/XMLSchema#"
    oda_ontology_url = "http://odahub.io/ontology#"

    if python_type == int:
        out_type = 'Integer'
        url_prefix = oda_ontology_url
    elif python_type == str:
        out_type = 'String'
        url_prefix = oda_ontology_url
    elif python_type == bool:
        out_type = 'Boolean'
        url_prefix = oda_ontology_url
    elif python_type == float:
        out_type = 'Float'
        url_prefix = oda_ontology_url
    else:
        url_prefix = xml_scheme_url

    output_url = f"{url_prefix}{out_type}"

    return output_url


def owl_type_for_python_type(python_type: type):
    out_type = python_type.__name__

    xml_scheme_url = "http://www.w3.org/2001/XMLSchema#"
    oda_ontology_url = "http://odahub.io/ontology#"

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
        url_prefix = oda_ontology_url

    output_url = f"{url_prefix}{out_type}"

    return output_url

class InputParameter:
    raw_line=None 
    name=None
    default_value=None
    python_type=None
    comment=None
    owl_type=None
    extra_ttl=None

    @classmethod
    def from_nbline(cls,line):
        parsed_nbline = parse_nbline(line)
        if parsed_nbline is None or parsed_nbline.get('name', None) is None:
            return None

        else:
            obj = cls()

            obj.raw_line = line            
            obj.name = parsed_nbline['name']
            obj.default_value = parsed_nbline['value']
            obj.python_type = parsed_nbline['python_type']
            obj.comment = parsed_nbline['comment']
            obj.owl_type = parsed_nbline['owl_type']
            obj.extra_ttl = parsed_nbline['extra_ttl']
            
            logger.info("interpreted %s %s %s comment: %s",
                    obj.name,
                    obj.default_value.__class__,obj.default_value,
                    obj.comment)

            if obj.owl_type is None:
                obj.owl_type = "http://www.w3.org/2001/XMLSchema#" + obj.python_type.__name__ # also use this if already defined

            return obj
    
        

    def as_dict(self):
        return dict(
                    default_value=self.default_value,
                    python_type=self.python_type,
                    name=self.name,
                    comment=self.comment,
                    owl_type=self.owl_type,
                    extra_ttl=self.extra_ttl
                )


class NotebookAdapter:
    limit_output_attachment_file = None

    def __init__(self, notebook_fn):
        self.notebook_fn = os.path.abspath(notebook_fn)
        self.name = notebook_short_name(notebook_fn)
        logger.debug("notebook adapter for %s", self.notebook_fn)
        logger.debug(self.extract_parameters())

    def new_tmpdir(self):
        logger.debug("tmpdir was "+getattr(self,'_tmpdir','unset'))
        self._tmpdir = None
        logger.debug("tmpdir became %s", self._tmpdir)

        return self.tmpdir

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

            url = subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=notebook_dir).decode().strip()
            revision = subprocess.check_output(["git", "describe", "--always", "--tags"], cwd=notebook_dir).decode().strip()

            self._notebook_origin = f"{url}#{revision}"
        
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
        return rdflib.URIRef(f"http://odahub.io/ontology#{self.unique_name}")


    def extract_parameters_from_cell(self, cell, G):
        parameters = {}

        for line in cell['source'].split("\n"):
            par = InputParameter.from_nbline(line)
            if par is not None:
                parameters[par.name] = par.as_dict()
                parameters[par.name]['value'] = par.as_dict()['default_value']
            else:
                p = parse_nbline(line, nb_uri=self.nb_uri)
                if p is not None:
                    try:
                        G.parse(data=p['extra_ttl'])
                    except Exception as e:
                        logger.warning("not a turtle: %s", p['extra_ttl'])

        return parameters


    def extract_parameters(self):
        nb = self.read()

        self.input_parameters = {}
        self.system_parameters = {}
        
        G = rdflib.Graph()
        
        for cell in nb.cells:
            for tag, attr in [
                    ('parameters', 'input_parameters'),
                    ('system-parameters', 'system_parameters'),
                    ('injected-parameters', 'input_parameters'),
                    ]:
                if tag in cell.metadata.get('tags', []):
                    pars = self.extract_parameters_from_cell(cell, G)
                    pars = {**getattr(self, attr), **pars}
                    setattr(self, attr, pars)
                                            
        for n, p in self.input_parameters.items():
            if p['extra_ttl'] is not None:
                G.parse(data=p['extra_ttl'])

        self.extra_ttl = G.serialize(format='turtle')

        return self.input_parameters

    
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

        

    def execute(self, parameters, progress_bar = True, log_output = True, inplace=False):
        t0 = time.time()
        logstasher.log(dict(origin="nb2workflow.execute", event="starting", parameters=parameters, workflow_name=notebook_short_name(self.notebook_fn), health=current_health()))

        logger.info("starting job")
        exceptions = self._execute(parameters, progress_bar, log_output, inplace)
            
        tspent = time.time() - t0
        logstasher.log(dict(origin="nb2workflow.execute", 
                            event="done", 
                            parameters=parameters, 
                            workflow_name=notebook_short_name(self.notebook_fn), 
                            exceptions=list(map(workflows.serialize_workflow_exception, exceptions)),
                            health=current_health(), 
                            time_spent=tspent))

        return exceptions

    def _execute(self, parameters, progress_bar = True, log_output = True, inplace=False):

        if not inplace :
            tmpdir = self.new_tmpdir()
            logger.info("new tmpdir: %s", tmpdir)

            try:
                output = subprocess.check_output(["git","clone", "--recurse-submodules", os.path.dirname(os.path.realpath(self.notebook_fn)), tmpdir])
                # output = subprocess.check_output(["git","clone", "--depth", "1", "file://" + os.path.dirname(os.path.realpath(self.notebook_fn)), tmpdir])
                logger.info("git clone output: %s", output)
            except Exception as e:
                logger.warning("git clone failed: %s, will attempt copytree", e)

                os.rmdir(tmpdir)

                shutil.copytree(os.path.dirname(os.path.realpath(self.notebook_fn)), tmpdir)
        else:
            tmpdir =os.path.dirname(os.path.realpath(self.notebook_fn))
            logger.info("executing inplace, no tmpdir is input dir: %s", tmpdir)

        
        self.update_summary(state="started", parameters=parameters)

        self.inject_output_gathering()
        exceptions = []

        ntries = 10
        while ntries > 0:
            try:
                pm.execute_notebook(
                   self.preproc_notebook_fn,
                   self.output_notebook_fn,
                   parameters = parameters,
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
            self.update_summary(state="failed", exceptions=list(map(workflows.serialize_workflow_exception, exceptions)))

        return exceptions

    def extract_pm_output(self):
        nb = sb.read_notebook(self.output_notebook_fn)

        outputs=dict()
        for i, d in nb.scraps.dataframe.iterrows():
            logger.debug("d... %s",d)
            #if d.dtype == "record":
            outputs[d['name']]=d['data']

        return outputs

    
    def extract_output_declarations(self):
        nb=self.read()

        outputs = {}

        for cell in nb.cells:
            if 'outputs' in cell.metadata.get('tags',[]):
                for line in cell['source'].split("\n"):
                    p = parse_nbline(line)
                    if p is None:
                        continue
                    outputs[p['name']] = p


        return outputs 

    def extract_output(self):
        return self.extract_pm_output()

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
        nb2w_store_base = os.getenv("NB2W_CACHE", os.getenv("HOME") + "/nb2w-store")
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
                        nba.extract_output_declarations())

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
        RuntimeError()

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
            json.dump(list(map(workflows.serialize_workflow_exception, exceptions)), f)

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
