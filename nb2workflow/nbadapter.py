from ast import literal_eval
import hashlib
import os
import sys
import glob
import shutil
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

import papermill as pm
import scrapbook as sb
import nbformat
from nbconvert import HTMLExporter

from nb2workflow.health import current_health
from nb2workflow import workflows
from nb2workflow.logging_setup import setup_logging

import logging
logger=logging.getLogger(__name__)


# try:
#     from nb2workflow import logstash
#     logstasher = logstash.LogStasher()
# except Exception as e:
#     logger.debug("unable to setup logstash %s",repr(e))

#     logstasher = None

logstasher = None

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

def understand_comment_references(comment):
    logger.debug("treating comment %s",comment)

    oda_ontology_prefix = "http://odahub.io/ontology"
    r = re.search(r"\b("+oda_ontology_prefix+r".*?)(?:\s+|$)", comment)
    if r:
        owl_type = r.groups()[0]
        logger.debug("comment contains owl references: %s",owl_type)
    else:
        owl_type = None
        logger.debug("no references in this comment")

    return dict(
        owl_type = owl_type,
    )


def parse_nbline(line):
    if line.strip()=="":
        return None
    elif line.strip().startswith("#"):
        logger.debug("found detached comment: \"%s\"",line)
        return None
    else:
        if "#" in line:
            assignment_line,comment=line.split("#",1)
        else:
            assignment_line=line
            comment=""
            
        if "=" in assignment_line:
            name, value_str = assignment_line.split("=", 1)
            name = name.strip()
        else:
            name = assignment_line.strip()
            value_str=None

        try:
            value=literal_eval(value_str.strip())
            python_type = type(value)
        except Exception:
            value = value_str
            python_type = str

        comment=comment

        return dict(
                    name = name,
                    value = value,
                    python_type = python_type,
                    comment = comment,
                    owl_type = understand_comment_references(comment).get('owl_type',None),
                )


class InputParameter:
    def __init__(self):
        pass

    @classmethod
    def from_nbline(cls,line):
        r = parse_nbline(line)
        if r is None:
            return r
        else:
            obj = cls()
            obj.raw_line=line

            p = parse_nbline(line)
            
            obj.name = p['name']
            obj.default_value = p['value']
            obj.python_type = p['python_type']
            obj.comment = p['comment']
            obj.owl_type = p['owl_type']

            obj.choose_owl_type()
            
            logger.debug("%s %s %s comment: %s",obj.name,obj.default_value.__class__,obj.default_value,obj.comment)
            return obj
    

    def choose_owl_type(self):
        self.owl_type = None

        if self.comment.strip() != "":
            references =  understand_comment_references(self.comment)

            if references.get('owl_type',None):
                self.owl_type = references.get('owl_type')

        if self.owl_type is None:
            self.owl_type = "http://www.w3.org/2001/XMLSchema#"+self.python_type.__name__ # also use this if already defined

    def as_dict(self):
        return dict(
                    default_value=self.default_value,
                    python_type=self.python_type,
                    name=self.name,
                    comment=self.comment,
                    owl_type=self.owl_type,
                )


class NotebookAdapter:
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

    def extract_parameters(self):
        nb=self.read()

        input_parameters = {}
        system_parameters = {}

        for cell in nb.cells:
            if 'parameters' in cell.metadata.get('tags',[]):
                for line in cell['source'].split("\n"):
                    par=InputParameter.from_nbline(line)
                    if par is not None:
                        input_parameters[par.name]=par.as_dict()
                        input_parameters[par.name]['value']=par.as_dict()['default_value']
            
            if 'system-parameters' in cell.metadata.get('tags',[]):
                for line in cell['source'].split("\n"):
                    par=InputParameter.from_nbline(line)
                    if par is not None:
                        system_parameters[par.name]=par.as_dict()
            
            if 'injected-parameters' in cell.metadata.get('tags',[]):
                for line in cell['source'].split("\n"):
                    par=InputParameter.from_nbline(line)
                    if par is not None:
                        input_parameters[par.name]['value']=par.as_dict()['default_value']

        self.system_parameters = system_parameters

        return input_parameters
    
    def interpret_parameters(self,parameters):
        expected_parameters=self.extract_parameters()
        request_parameters=dict()

        unexpected_parameters=[]
        issues=[]

        for arg in parameters:
            if arg.startswith("_"): continue

            logger.info("request arg %s",parameters[arg])
            if arg in expected_parameters:
                try:
                    request_parameters[arg]=cast_parameter(parameters.get(arg),expected_parameters.get(arg))
                    logger.info("request arg %s provided as %s",parameters[arg],request_parameters[arg])
                except ValueError as e:
                    issues.append(e.args[0])
            else:
                unexpected_parameters.append(arg)

        if len(unexpected_parameters)>0:
            issues+=[f'found unexpected request parameters: {", ".join(unexpected_parameters)}, can be {", ".join(expected_parameters.keys())}']
            
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
            self._summary['state'] = self._summary.get("state",[]) + [(time.time(), state)]

        fn = os.path.join(self.tmpdir, "summary.yaml")
        yaml.dump(self._summary, open(fn, "w"))

        

    def execute(self, parameters, progress_bar = True, log_output = True, inplace=False):
        t0 = time.time()
        if logstasher is not None:
            logstasher.log(dict(origin="nb2workflow.execute", event="starting", parameters=parameters, workflow_name=notebook_short_name(self.notebook_fn), health=current_health()))

        logger.info("starting job")
        exceptions = self._execute(parameters, progress_bar, log_output, inplace)
            
        tspent = time.time() - t0
        if logstasher is not None:
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
                output = subprocess.check_output(["git","clone",os.path.dirname(os.path.realpath(self.notebook_fn)), tmpdir])
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
                    if p is None: continue
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
import os
    
from nb2workflow.nbadapter import denumpyfy

"""
        for output in outputs.keys():
            logger.debug("output: %s",output)
            output_gather_content+="""
try:
    sb.glue("{output}",denumpyfy({output}))
except Exception as e:
    print("failed to glue {output}", {output})
    print("will glue jsonified")
    sb.glue("{output}",json.dumps(denumpyfy({output})))
""".format(output=output)

            output_gather_content+="\nisinstance({output},str) and os.path.exists({output}) and sb.glue(\"{output}_content\",base64.b64encode(open({output},'rb').read()).decode())".format(output=output)
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


def notebook_short_name(ipynb_fn):
    return os.path.basename(ipynb_fn).replace(".ipynb","")

def find_notebooks(source, tests=False) -> dict[str, NotebookAdapter]:

    base_filter = lambda fn: "output" not in fn and "preproc" not in fn

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
        notebook_adapters={notebook_short_name(source): NotebookAdapter(source)}

    else:
        raise Exception("requested notebook not found:",source)

    return notebook_adapters

def nbinspect(nb_source, out=True, machine_readable=False):
    nbas = find_notebooks(nb_source)

    class CustomEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, type):
                return str(obj)
            return json.JSONEncoder.default(self, obj)

    summary = []

    for n, nba in nbas.items():
        summary.append({
                "parameters": nba.extract_parameters(),
                "outputs": nba.extract_output_declarations()
            })
        print(json.dumps(summary[-1], indent=4, sort_keys=True, cls=CustomEncoder))

    if machine_readable:
        print("WORKFLOW-NB-SIGNATURE:", json.dumps(summary, cls=CustomEncoder))


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
