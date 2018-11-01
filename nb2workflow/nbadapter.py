import papermill as pm
import nbformat
from ast import literal_eval


class NotebookAdapter:
    def __init__(self,notebook_fn):
        self.notebook_fn=notebook_fn

    def output_notebook_fn(self):
        return 'output_'+self.notebook_fn

    def extract_parameters(self):
        nb=nbformat.reads(open(self.notebook_fn).read(), as_version=4)

        input_parameters=[]

        for cell in nb.cells:
            if 'parameters' in cell.metadata.get('tags',[]):
                for row in cell['source'].split("\n"):
                    if row.strip()!="":
                        parameter,default_str=row.split("=")
                        default=literal_eval(default_str.strip())
                        print(parameter,default.__class__,default)
                        input_parameters.append(dict(
                            name = parameter,
                            python_type = type(default),
                            default_value = default,
                        ))

        return input_parameters

    def execute(self):
        pm.execute_notebook(
           self.notebook_fn,
           self.output_notebook_fn,
           parameters = dict(det='L1')
        )

    def extract_output(self):
        nb = pm.read_notebook(self.output_notebook_fn)

        for i, d in nb.dataframe.iterrows():
            if d.type == "record":
                print d.name, d.type, d.value


    #extract_parameters()
    #execute()
    #extract_output()
