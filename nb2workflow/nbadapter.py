from ast import literal_eval
import os
import glob

import papermill as pm
import nbformat

import logging
logger=logging.getLogger(__name__)

def owlify_type(python_type):
    return "http://odahub.io/ontology/types/"+python_type.__name__
    
def cast_parameter(x,par):
    logger.debug("cast %s %s",x,par)
    return par['python_type'](x)

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
                for row in cell['source'].split("\n"):
                    if row.strip()!="":
                        parameter,default_str=row.split("=")
                        default=literal_eval(default_str.strip())
                        logger.debug("%s %s %s",parameter,default.__class__,default)
                        input_parameters[parameter]=dict(
                            python_type = type(default),
                            owl_type=owlify_type(type(default)),
                            default_value = default,
                        )

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


    elif os.path.isfile(args.notebook):
        notebook_adapters={notebook_short_name(args.notebook),NotebookAdapter(args.notebook)}

    else:
        raise Exception("requested notebook not found:",source)

    return notebook_adapters

