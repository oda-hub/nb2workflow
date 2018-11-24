from ast import literal_eval
import os
import glob
import re
import tempfile

import papermill as pm
import nbformat

import logging
logger=logging.getLogger(__name__)

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
            name, value_str=assignment_line.split("=", 1)
            name = name.strip()
        else:
            name = assignment_line
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
            self.owl_type = "http://odahub.io/ontology/types/"+self.python_type.__name__ # also use this if already defined

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
    
    @property
    def tmpdir(self):
        if not hasattr(self,'_tmpdir'):
            self._tmpdir = tempfile.mkdtemp()
        return self._tmpdir
    
    @property
    def preproc_notebook_fn(self):
        return os.path.join(self.tmpdir,os.path.basename(self.notebook_fn.replace(".ipynb","_preproc.ipynb")))

    @property
    def output_notebook_fn(self):
        return os.path.join(self.tmpdir,os.path.basename(self.notebook_fn.replace(".ipynb","_output.ipynb")))

    def extract_parameters(self):
        nb=nbformat.reads(open(self.notebook_fn).read(), as_version=4)

        input_parameters={}

        for cell in nb.cells:
            if 'parameters' in cell.metadata.get('tags',[]):
                for line in cell['source'].split("\n"):
                    par=InputParameter.from_nbline(line)
                    if par is not None:
                        input_parameters[par.name]=par.as_dict()

        return input_parameters
    
    def interpret_parameters(self,parameters):
        expected_parameters=self.extract_parameters()
        request_parameters=dict()

        unexpected_parameters=[]
        for arg in parameters:
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

    @property
    def exceptions(self):
        if not hasattr(self,'_exceptions'):
            self._exceptions = []
        return self._exceptions

    def execute(self, parameters):
        self.inject_output_gathering()

        try:
            pm.execute_notebook(
               self.preproc_notebook_fn,
               self.output_notebook_fn,
               parameters = parameters,
            )
        except pm.PapermillExecutionError as e:
            self.exceptions.append(e)
            logger.debug(e)
            logger.debug(e.args)

    def extract_pm_output(self):
        nb = pm.read_notebook(self.output_notebook_fn)

        outputs=dict()
        for i, d in nb.dataframe.iterrows():
            logger.debug("d... %s",d)
            if d.type == "record":
                outputs[d['name']]=d['value']

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
import base64
import os

"""
        for output in outputs.keys():
            logger.debug("output: %s",output)
            output_gather_content+="\npm.record(\"{output}\",{output})".format(output=output)

            output_gather_content+="\nisinstance({output},str) and os.path.exists({output}) and pm.record(\"{output}_content\",base64.b64encode(open({output}).read()))".format(output=output)
            output_gather_content+="\n".format(output=output)
            #output_gather_content+="pm.record(\"{}\",dict(filename=fn,content=base64.b64encode(open(fn).read())))"
        #"pm.record(\"{}\",dict(filename=fn,content=base64.b64encode(open(fn).read())))"

        newcell = nbformat.v4.new_code_cell(source=output_gather_content)
        newcell.metadata['tags'] = ['injected-gather-outputs']

        nb=nbformat.reads(open(self.notebook_fn).read(), as_version=4)
        nb.cells = nb.cells + [newcell] 

        pm.iorw.write_ipynb(nb, self.preproc_notebook_fn)


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

