from ast import literal_eval
import os
import sys
import glob
import yaml 
import re
import time
import tempfile
import subprocess
import ruamel.yaml as yaml
import argparse
import json

import papermill as pm
import scrapbook as sb
import nbformat

from nb2workflow.health import current_health
from nb2workflow import workflows

import logging
logger=logging.getLogger(__name__)


try:
    from nb2workflow import logstash
    logstasher = logstash.LogStasher()
except Exception as e:
    logger.warning("unable to setup logstash %s",repr(e))

    logstasher = None


class PapermillWorkflowIncomplete(Exception):
    pass


def cast_parameter(x,par):
    logger.debug("cast %s %s",x,par)
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
        except Exception as e:
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
    def __init__(self,notebook_fn):
        self.notebook_fn = notebook_fn
        self.name = notebook_short_name(notebook_fn)
        logger.debug("notebook adapter for %s",notebook_fn)
        logger.debug(self.extract_parameters())


    def new_tmpdir(self):
        logger.debug("tmpdir was "+getattr(self,'_tmpdir','unset'))
        self._tmpdir = None
        new_tmpdir = self.tmpdir
        logger.debug("tmpdir became "+self._tmpdir)

        return self.tmpdir

    @property
    def tmpdir(self):
        if getattr(self,'_tmpdir', None) is None:
            self._tmpdir = tempfile.mkdtemp(prefix="nb2w-")
        return self._tmpdir
    
    @property
    def preproc_notebook_fn(self):
        return os.path.join(self.tmpdir,os.path.basename(self.notebook_fn.replace(".ipynb","_preproc.ipynb")))

    @property
    def output_notebook_fn(self):
        return os.path.join(self.tmpdir,os.path.basename(self.notebook_fn.replace(".ipynb","_output.ipynb")))

    def extract_parameters(self):
        nb=nbformat.reads(open(self.notebook_fn).read(), as_version=4)

        input_parameters = {}
        system_parameters = {}

        for cell in nb.cells:
            if 'parameters' in cell.metadata.get('tags',[]):
                for line in cell['source'].split("\n"):
                    par=InputParameter.from_nbline(line)
                    if par is not None:
                        input_parameters[par.name]=par.as_dict()
                        input_parameters[par.name]=par.as_dict()
            
            if 'system-parameters' in cell.metadata.get('tags',[]):
                for line in cell['source'].split("\n"):
                    par=InputParameter.from_nbline(line)
                    if par is not None:
                        system_parameters[par.name]=par.as_dict()
                        system_parameters[par.name]=par.as_dict()

        self.system_parameters = system_parameters

        return input_parameters
    
    def interpret_parameters(self,parameters):
        expected_parameters=self.extract_parameters()
        request_parameters=dict()

        unexpected_parameters=[]
        for arg in parameters:
            if arg.startswith("_"): continue

            logger.info("request arg %s",parameters[arg])
            if arg in expected_parameters:
                request_parameters[arg]=cast_parameter(parameters.get(arg),expected_parameters.get(arg))
                logger.info("request arg %s provided as %s",parameters[arg],request_parameters[arg])
            else:
                unexpected_parameters.append(arg)

        issues=[]

        if len(unexpected_parameters)>0:
            issues+=["found unexpected request parameters: "+(", ".join(unexpected_parameters))]
            
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

        

    def execute(self, parameters, progress_bar = True, log_output = True):
        t0 = time.time()
        if logstasher is not None:
            logstasher.log(dict(origin="nb2workflow.execute", event="starting", parameters=parameters, workflow_name=notebook_short_name(self.notebook_fn), health=current_health()))


        exceptions = self._execute(parameters, progress_bar, log_output)

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

    def _execute(self, parameters, progress_bar = True, log_output = True):
        tmpdir = self.new_tmpdir()

        logger.info("new tmpdir: %s", tmpdir)

        logger.info(subprocess.check_output(["git","clone",os.path.dirname(os.path.realpath(self.notebook_fn)), tmpdir]))
        
        self.update_summary(state="started", parameters=parameters)

        self.inject_output_gathering()
        exceptions = []

        
#        root = logging.getLogger()
#        root.setLevel(logging.DEBUG)

#        handler = logging.StreamHandler()
#        handler.setLevel(logging.DEBUG)
#        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#        handler.setFormatter(formatter)
#        root.addHandler(handler)

#        root.info("towards excution")


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
                logger.info("retrying...", ntries)
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
        nb=nbformat.reads(open(self.notebook_fn).read(), as_version=4)

        outputs = {}

        for cell in nb.cells:
            if 'outputs' in cell.metadata.get('tags',[]):
                for line in cell['source'].split("\n"):
                    p = parse_nbline(line)
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
import os

"""
        for output in outputs.keys():
            logger.debug("output: %s",output)
            output_gather_content+="\nsb.glue(\"{output}\",{output})".format(output=output)

            output_gather_content+="\nisinstance({output},str) and os.path.exists({output}) and sb.glue(\"{output}_content\",base64.b64encode(open({output},'rb').read()).decode())".format(output=output)
            output_gather_content+="\n".format(output=output)

        newcell = nbformat.v4.new_code_cell(source=output_gather_content)
        newcell.metadata['tags'] = ['injected-gather-outputs']

        nb=nbformat.reads(open(self.notebook_fn).read(), as_version=4)
        nb.cells = nb.cells + [newcell] 

        pm.iorw.write_ipynb(nb, self.preproc_notebook_fn)

    def get_system_parameter_value(self, name, default):
        if name in self.system_parameters:
            return self.system_parameters.pop(name)['default_value'] 

        return default


def notebook_short_name(ipynb_fn):
    return os.path.basename(ipynb_fn).replace(".ipynb","")

def find_notebooks(source):

    if os.path.isdir(source):
        notebooks=[ fn for fn in glob.glob(source+"/*ipynb") if "output" not in fn and "preproc" not in fn ]
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


def nbrun(nb_source, inp):

    nbas = find_notebooks(nb_source)

    if len(nbas) > 1:
        nba = nbas[inp.pop('notebook')]
    elif len(nbas) == 1:
        nba = list(nbas.values())[0]

    r = nba.interpret_parameters(inp)
    
    if r['issues'] != []:
        raise Exception(r['issues'])

    pars = r['request_parameters']

    logging.info("found parameters %s", repr(pars))

    nba.execute(pars)

    r={}
    for k,v in nba.extract_output().items():
        r[k.strip()]=repr(v)

    with open("cwl.output.json", "w") as f:
        json.dump(r, f)

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('--debug', action="store_true")
    
    parser.add_argument('inputs', nargs=argparse.REMAINDER)

    args = parser.parse_args()

    inputs={}
    for i in args.inputs:
        if not i.startswith("--inp-"): continue
        k,v = i.replace('--inp-','').split("=")
        inputs[k] = v
        

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    root = logging.getLogger()

    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    if args.debug:
        root.setLevel(logging.DEBUG)
        handler.setLevel(logging.DEBUG)
    else:
        root.setLevel(logging.INFO)
        handler.setLevel(logging.INFO)


    nbrun(args.notebook, inputs)


if __name__ == "__main__":
    main()
