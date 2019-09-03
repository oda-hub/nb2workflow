import argparse
import logging

import cwlgen

import nb2workflow.nbadapter as nbadapter

def nb2cwl(notebook_fn, cwl_fn):
    nba = nbadapter.NotebookAdapter(notebook_fn)

    tool_object = cwlgen.CommandLineTool(
                    tool_id="papermill", 
                    base_command="echo", 
                    label=None, 
                    doc=None,
                    cwl_version="v1.0", 
                    stdin=None,
                    stderr=None, 
                    stdout=None, 
                    path=None)

    tool_object.inputs.append(
        cwlgen.CommandInputParameter(
                     "myParamId", 
                     param_type="string", 
                     label=None, 
                     secondary_files=None, 
                     param_format=None,
                     streamable=None, 
                     doc=None, 
                     input_binding=None, 
                     default=None)
    )

    tool_object.export()

    tool_object.export(cwl_fn)

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('cwl', metavar='cwl', type=str)
    parser.add_argument('--publish', metavar='upstream-url', type=str, default=None)
    parser.add_argument('--publish-as', metavar='published url', type=str, default=None)
    parser.add_argument('--debug', action="store_true")
    parser.add_argument('--container', action="store_true")

    args = parser.parse_args()

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

    nb2cwl(args.notebook, args.cwl)
