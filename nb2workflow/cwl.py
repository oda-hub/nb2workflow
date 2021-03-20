import argparse
import logging
import os

import cwlgen

import nb2workflow.nbadapter as nbadapter


def python_type2cwl_type(pt):
    if pt == str:
        return 'string'
    elif pt == bool:
        return 'boolean'

    return pt.__name__


def nb2cwl_container(image, notebook_fn, cwl_fn, command=None, nbrunner_module=None):
    nba = nbadapter.NotebookAdapter(notebook_fn)

    if command is None:
        base_command = "python"
        if nbrunner_module is None:
            arguments = ["-m", "nb2workflow.nbadapter", "/repo/"+nba.name]
        else:
            arguments = ["-m", nbrunner_module, "/repo/"+nba.name]
    else:
        base_command = 'bash'
        arguments = ['-c', command]

    tool_object = cwlgen.CommandLineTool(
        tool_id=nba.name,
        base_command=base_command,
        label=None,
        doc=None,
        cwl_version="v1.0",
        stdin=None,
        stderr=None,
        stdout=None,
        path=None)

    notebook_name = os.path.basename(notebook_fn)

    tool_object.arguments = arguments

    tool_object.requirements.append(
        cwlgen.DockerRequirement(docker_pull=image))

    for par in nba.extract_parameters().values():
        tool_object.inputs.append(
            cwlgen.CommandInputParameter(
                par['name'],
                param_type=python_type2cwl_type(par['python_type']),
                label=None,
                secondary_files=None,
                param_format=None,
                streamable=None,
                doc=None,
                input_binding=dict(prefix="--inp-"+par['name'], separate=True),
                default=par['default_value'],
            )
        )

    # tool_object.outputs.append(
    #    cwlgen.CommandOutputParameter('log',
    #                                  param_type='stdout',
    #                                  doc='log')
    # )

    for n, o in nba.extract_output_declarations().items():
        tool_object.outputs.append(
            cwlgen.CommandOutputParameter(n,
                                          param_type='string',
                                          doc='lines found with the pattern')
        )

    tool_object.export()
    tool_object.export(cwl_fn)


def nb2cwl(notebook_fn, cwl_fn, nbrunner_module="nb2workflow.nbadapter"):
    nba = nbadapter.NotebookAdapter(notebook_fn)

    tool_object = cwlgen.CommandLineTool(
        tool_id=nba.name,
        base_command="python",
        label=None,
        doc=None,
        cwl_version="v1.0",
        stdin=None,
        stderr=None,
        stdout=None,
        path=None)

    tool_object.arguments = ["-m", nbrunner_module, notebook_fn]

    for par in nba.extract_parameters().values():
        tool_object.inputs.append(
            cwlgen.CommandInputParameter(
                par['name'],
                param_type=python_type2cwl_type(par['python_type']),
                label=None,
                secondary_files=None,
                param_format=None,
                streamable=None,
                doc=None,
                input_binding=dict(
                    prefix="--inp-"+par['name']+"=", separate=False),
                default=par['default_value'],
            )
        )

    # tool_object.outputs.append(
    #    cwlgen.CommandOutputParameter('log',
    #                                  param_type='stdout',
    #                                  doc='log')
    # )

    for n, o in nba.extract_output_declarations().items():
        tool_object.outputs.append(
            cwlgen.CommandOutputParameter(n,
                                          param_type='string',
                                          doc='lines found with the pattern')
        )

    tool_object.export()
    tool_object.export(cwl_fn)


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('cwl', metavar='cwl', type=str)
    parser.add_argument('--publish', metavar='upstream-url',
                        type=str, default=None)
    parser.add_argument(
        '--publish-as', metavar='published url', type=str, default=None)
    parser.add_argument('--debug', action="store_true")
    parser.add_argument('--container', metavar="container")
    parser.add_argument(
        '--nbrunner-module', metavar="nbrunner_module", default="nb2workflow.nbadapter")

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

    if args.container:
        nb2cwl_container(args.container, os.path.basename(
            args.notebook), args.cwl, nbrunner_module=args.nbrunner_module)
    else:
        nb2cwl(args.notebook, args.cwl, nbrunner_module=args.nbrunner_module)


if __name__ == "__main__":
    main()
