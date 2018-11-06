from ast import literal_eval
import os
import glob

import papermill as pm
import nbformat

import logging
logger=logging.getLogger(__name__)

def cast_parameter(x,par):
    logger.debug("cast %s %s",x,par)
    return par['python_type'](x)


class InputParameter:
    def __init__(self):
        pass

    @classmethod
    def from_nbline(cls,line):
        if line.strip()=="":
            return None
        elif line.strip().startswith("#"):
            logger.debug("found detached comment: \"%s\"",line)
            return None
        else:
            obj=cls()
            obj.raw_line=line

            if "#" in line:
                assignment_line,comment=line.split("#",1)
            else:
                assignment_line=line
                comment=""
                
            obj.name,default_str=assignment_line.split("=")
            obj.default_value=literal_eval(default_str.strip())
            obj.python_type = type(obj.default_value)
            
            logger.debug("%s %s %s comment: %s",obj.name,obj.default_value.__class__,obj.default_value,comment)
            return obj

    @property
    def owl_type(self):
        return "http://odahub.io/ontology/types/"+self.python_type.__name__

    def as_dict(self):
        return dict(
                    default_value=self.default_value,
                    python_type=self.python_type,
                    name=self.name,
                )


class NotebookAdapter:
    def __init__(self,notebook_fn):
        self.notebook_fn=notebook_fn
        logger.debug("notebook adapter for %s",notebook_fn)
        logger.debug(self.extract_parameters())

    @property
    def output_notebook_fn(self):
        return self.notebook_fn.replace(".ipynb","_output.ipynb")

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
            else:
                unexpected_parameters.append(arg)

        issues=[]

        if len(unexpected_parameters)>0:
            issues+=["found unexpected request parameters: "+(", ".join(unexpected_parameters))]
            
        return dict(
                        issues=issues,
                        request_parameters=request_parameters,
                    )



    def execute(self, parameters):
        pm.execute_notebook(
           self.notebook_fn,
           self.output_notebook_fn,
           parameters = parameters,
        )

    def extract_pm_output(self):
        nb = pm.read_notebook(self.output_notebook_fn)

        outputs=dict()
        for i, d in nb.dataframe.iterrows():
            logger.debug("d... %s",d)
            if d.type == "record":
                outputs[d['name']]=d['value']

        return outputs


    def extract_output(self):
        return self.extract_pm_output()

def notebook_short_name(ipynb_fn):
    return os.path.basename(ipynb_fn).replace(".ipynb","")

def find_notebooks(source):

    if os.path.isdir(source):
        notebooks=[ fn for fn in glob.glob(source+"/*ipynb") if "output" not in fn ]
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

