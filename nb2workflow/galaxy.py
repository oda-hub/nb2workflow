from __future__ import annotations

import xml.etree.ElementTree as ET
import os
import re
import shutil
from textwrap import dedent
import argparse
import logging
import requests
from typing import TypeAlias

import yaml
import json

from oda_api.ontology_helper import Ontology
from nb2workflow.nbadapter import NotebookAdapter, find_notebooks, oda_ontology_path
from nb2workflow.nbadapter import ontology as nba_ontology_global_var

import nbformat
from nbconvert.exporters import ScriptExporter

import subprocess as sp

import bibtexparser as bib
import pypandoc

import tempfile
import black
import autoflake
import isort
import validators

logger = logging.getLogger()


global_req = []

_success_text = '*** Job finished successfully ***'

OntoPathOrObj: TypeAlias = str | os.PathLike | Ontology | None

class GalaxyParameter:
    def __init__(self, 
                 name, 
                 python_type,
                 is_dataset=False,
                 description=None,
                 default_value=None, 
                 min_value=None, 
                 max_value=None, 
                 allowed_values=None,
                 additional_attrs=None):
        
        #TODO: type is fully defined by owl_type. Use it instead?  
        partype_lookup = {str: 'text', 
                    bool: 'boolean',
                    float: 'float',
                    int: 'integer',
                    dict: 'data',
                    list: 'data'} 
        
        partype = partype_lookup[python_type]
        
        is_json_input = False

        if allowed_values is not None:
            partype = 'select'
        
        if is_dataset:
            partype = 'data'
            self.original_value = default_value
            # TODO: actual formats after implementing in ontology
            self.data_format = 'data'
            default_value = None
        
        if python_type in [dict, list]:
            self.data_format = 'json'
            self.original_value = default_value
            default_value = None
            is_json_input = True


        self.name = name
        self.partype = partype
        self.description=description
        self.default_value = default_value
        self.min_value = min_value
        self.max_value = max_value
        self.allowed_values = allowed_values
        self.additional_attrs = additional_attrs
        self.is_json_input = is_json_input
                
    
    @classmethod
    def from_inspect(cls, par_details, ontology: OntoPathOrObj = None):
        label = None
        par_format = None
        par_unit = None
        min_value = None
        max_value = None
        allowed_values = None
        is_dataset = False
        is_optional = par_details.get('is_optional', False)
        
        owl_uri = par_details['owl_type']

        is_posix_path, is_file_url, is_file_ref = NotebookAdapter.check_is_file_input(owl_uri).values()
        if is_posix_path or (is_file_ref and not is_file_url):
            is_dataset = True
            
        if ontology is not None:
            # NOTE: explicit here while Ontology.is_ontology_available 
            #       and empty Ontology aren't really implemented
            #       but want to allow None for basic for non-mmoda conversions
            #       (default ontology is still used in nbadapter in fact)

            onto = ontology if isinstance(ontology, Ontology) else Ontology(ontology)
                        
            if par_details.get('extra_ttl') is not None:
                onto.parse_extra_triples(par_details['extra_ttl'])
            
            label = onto.get_oda_label(owl_uri) # TODO: there are both label and description, also groups
            
            if not is_dataset:
                par_format = onto.get_parameter_format(owl_uri)
                par_unit = onto.get_parameter_unit(owl_uri)
                min_value, max_value = onto.get_limits(owl_uri)
                allowed_values = onto.get_allowed_values(owl_uri)
                
        description = label if label is not None else par_details['name']
        if par_format is not None:
            description += f" (format: {par_format})"
        if par_unit is not None:
            description += f" (unit: {par_unit})"
        
        return cls(par_details['name'], 
                   par_details['python_type'], 
                   is_dataset=is_dataset,
                   description=description,
                   default_value=par_details['default_value'], 
                   min_value=min_value,
                   max_value=max_value,
                   allowed_values=allowed_values,
                   additional_attrs={'optional': 'true' if is_optional else 'false'})

    def to_xml_tree(self):
        
        # TODO: consider using https://github.com/hexylena/galaxyxml
        
        attrs = {'name': self.name,
                 'type': self.partype}
        if self.default_value is not None and self.partype not in ['select', 'boolean']:
            attrs['value'] = str(self.default_value)
        if self.partype == 'boolean' and self.default_value:
            attrs['checked'] = 'true'

        if self.description is not None:
            attrs['label'] = self.description

        if self.min_value is not None:
            attrs['min'] = str(self.min_value)
        if self.max_value is not None:
            attrs['max'] = str(self.max_value)

        if getattr(self, 'data_format', None) is not None:
            attrs['format'] = self.data_format
            
        if self.additional_attrs is not None:
            attrs.update(self.additional_attrs)
                    
        element = ET.Element('param',
                             **attrs)
        
        if self.allowed_values is not None:
            for val in self.allowed_values:
                attrs = {'value': str(val)}
                if val == self.default_value:
                    attrs['selected'] = 'true'
                option = ET.SubElement(element, 'option', attrib=attrs)
                option.text = str(val)
        # TODO: do we need additional validation?
        
        return element

class GalaxyOutput:
    def __init__(self, name, is_oda, dprod=None):
        self.name = name
        if dprod is None:
            dprod=''
            self.dprod = dprod
        else:
            self.dprod = dprod
            dprod += '_'
        self.dataname = f"out_{dprod.replace('-', '_').replace(' ', '_')}{self.name}"
        self.is_oda = is_oda
        self.outfile_name = f"{name}_galaxy.output"
    
    @classmethod
    def from_inspect(cls, outp_details, ontology: OntoPathOrObj = None, dprod=None):
        owl_uri = outp_details['owl_type']        
        is_oda = False 

        if ontology is not None:
            onto = ontology if isinstance(ontology, Ontology) else Ontology(ontology)

            if outp_details['extra_ttl'] is not None:
                onto.parse_extra_triples(outp_details['extra_ttl'])
        
            if owl_uri is not None and onto.is_data_product(owl_uri, include_parameter_products=False):
                is_oda = True
        
        return cls(outp_details['name'], is_oda, dprod)

    def to_xml_tree(self):
        if self.dprod is None:
            label = "${tool.name} -> %s"%self.name 
        else:
            label = "${tool.name} -> %s %s"%(self.dprod, self.name)
            
        attrs = {'label': label,
                 'name': self.dataname,
                 #'auto_format': 'true', 
                 'format': 'auto',
                 'from_work_dir': self.outfile_name}
        
        element = ET.Element('data', attrib=attrs)

        return element


def _nb2script(nba: NotebookAdapter, inputs: list[GalaxyParameter], outputs: list[GalaxyOutput]):
    input_nb = nba.notebook_fn
    mynb = nbformat.read(input_nb, as_version=4)

    oda_outp_code = ''
    simple_outp_code = ''
    for outp in outputs:
        if outp.is_oda:
            oda_outp_code += f"_oda_outs.append(('{outp.dataname}', '{outp.outfile_name}', {outp.name}))\n"
        else:
            simple_outp_code += f"_simple_outs.append(('{outp.dataname}', '{outp.outfile_name}', {outp.name}))\n"
    
    import_code = dedent( """
                #!/usr/bin/env python
                
                # This script is generated with nb2galaxy
                         
                # flake8: noqa         

                import json
                import os
                import shutil
                """)
    if oda_outp_code:
        import_code += "from oda_api.json import CustomJSONEncoder\n"
            
    inject_import = nbformat.v4.new_code_cell(import_code)
    inject_import.metadata['tags'] = ['injected_import']
    mynb.cells.insert(0, inject_import)
    
    inject_pos = 0
    for pos, cell in enumerate(mynb.cells):
        if 'parameters' in cell['metadata'].get('tags', []):
            inject_pos = pos + 1
            break
    # NOTE: validation of args is external


    input_code = dedent("""
        _galaxy_wd = os.getcwd()

        with open('inputs.json', 'r') as fd:
            inp_dic = json.load(fd)
        if 'C_data_product_' in inp_dic.keys():
            inp_pdic = inp_dic['C_data_product_']
        else:
            inp_pdic = inp_dic
        """)

    simple_par_names = [x.name for x in inputs if not x.is_json_input]
    struct_par_names = [x.name for x in inputs if x.is_json_input]

    if len(simple_par_names)>0:
        for par_name in simple_par_names:
            if nba.input_parameters[par_name]['is_optional']:
                input_code += dedent(f"""
                    {par_name} = (
                        {nba.input_parameters[par_name]['python_type'].__name__}(inp_pdic['{par_name}']) 
                        if inp_pdic.get('{par_name}', None) is not None 
                        else None
                        )

                    """)
            else:
                input_code += f"{par_name} = {nba.input_parameters[par_name]['python_type'].__name__}(inp_pdic['{par_name}'])\n"


    if len(struct_par_names)>0:
        input_code += dedent(f"""
            for _vn in {struct_par_names}:
                if inp_pdic.get(_vn, None) is not None:
                    with open(inp_pdic[_vn], 'r') as _this_fd:
                        _vv = json.load(_this_fd)
                        globals()[_vn] = _vv
                else:
                    globals()[_vn] = None
        """)

    inject_read = nbformat.v4.new_code_cell(input_code)

    inject_read.metadata['tags'] = ['injected-input']
    mynb.cells.insert(inject_pos, inject_read)
 
    exporter = ScriptExporter()
    script, resources = exporter.from_notebook_node(mynb)
    
    outp_code = dedent("""
                    # output gathering
                    _galaxy_meta_data = {}
                    """)

    if oda_outp_code:
        outp_code += "_oda_outs = []\n"
        outp_code += oda_outp_code
        outp_code += dedent("""
                for _outn, _outfn, _outv in _oda_outs:
                    _galaxy_outfile_name = os.path.join(_galaxy_wd, _outfn)
                    if isinstance(_outv, str) and os.path.isfile(_outv):
                        shutil.move(_outv, _galaxy_outfile_name)
                        _galaxy_meta_data[_outn] = {'ext': '_sniff_'}
                    elif getattr(_outv, "write_fits_file", None):
                        _outv.write_fits_file(_galaxy_outfile_name)
                        _galaxy_meta_data[_outn] = {'ext': 'fits'}
                    elif getattr(_outv, "write_file", None):
                        _outv.write_file(_galaxy_outfile_name)
                        _galaxy_meta_data[_outn] = {'ext': '_sniff_'}
                    else:
                        with open(_galaxy_outfile_name, 'w') as fd:
                            json.dump(_outv, fd, cls=CustomJSONEncoder)
                        _galaxy_meta_data[_outn] = {'ext': 'json'}
                """)        
    if simple_outp_code:
        outp_code += "_simple_outs = []\n"
        outp_code += simple_outp_code
        
        if re.search(r'^\s*import numpy as np', script, flags=re.M):
            outp_code += "_numpy_available = True\n"
        else:
            outp_code += dedent("""
                    try:
                        import numpy as np   # noqa: E402
                        _numpy_available = True
                    except ImportError:
                        _numpy_available = False
                    """)
        
        outp_code += dedent("""
                for _outn, _outfn, _outv in _simple_outs:
                    _galaxy_outfile_name = os.path.join(_galaxy_wd, _outfn)
                    if isinstance(_outv, str) and os.path.isfile(_outv):
                        shutil.move(_outv, _galaxy_outfile_name)
                        _galaxy_meta_data[_outn] = {'ext': '_sniff_'}
                    elif _numpy_available and isinstance(_outv, np.ndarray):
                        with open(_galaxy_outfile_name, 'wb') as fd:
                            np.savez(fd, _outv)
                        _galaxy_meta_data[_outn] = {'ext': 'npz'}
                    else:
                        with open(_galaxy_outfile_name, 'w') as fd:
                            json.dump(_outv, fd)
                        _galaxy_meta_data[_outn] = {'ext': 'expression.json'}
                """)

    outp_code += dedent(f"""
                        with open(os.path.join(_galaxy_wd, 'galaxy.json'), 'w') as fd:
                            json.dump(_galaxy_meta_data, fd)
                        print("{_success_text}")
                        """)
    
    script += outp_code
    
    # restyling
    script = re.sub(r'^# In\[[\d\s]*\]:$', '', script, flags=re.M)
    
    script = autoflake.fix_code(script, remove_all_unused_imports=True, remove_unused_variables=True)
    
    script = isort.api.sort_code_string(script, config=isort.Config(profile="black"))
        
    BLACK_MODE = black.Mode(target_versions={black.TargetVersion.PY39}, line_length=79) # type: ignore
    try:
        script = black.format_file_contents(script, fast=False, mode=BLACK_MODE)
    except black.NothingChanged: # type: ignore
        pass
    finally:
        if script[-1] != "\n":
            script += "\n"
    
    script = re.sub(r'^(.*get_ipython\(\).*)$', r'\1   # noqa: F821', script, flags=re.M)
    script = re.sub(r'^(\s*display\(.*)$', r'\1   # noqa: F821', script, flags=re.M)
    script = re.sub(r'(?<=^\n)\n', '', script, flags=re.M)
        
    return script


class Requirements:

    def __init__(self, available_channels, conda_env_yml = None, requirements_txt = None, extra_req = []):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.fullenv_file_path = os.path.join(self.tmpdir.name, 'environment.yml')

        self.micromamba = self._get_micromamba_binary()

        boilerplate_env_dict = {'channels': available_channels, 'dependencies': extra_req}

        if conda_env_yml is not None:
            with open(conda_env_yml, 'r') as fd:
                self.env_dict = yaml.safe_load(fd)

            if self.env_dict.get('dependencies'):
                if self.env_dict.get('channels'):
                    extra_channels = set(self.env_dict['channels']) - set(available_channels)
                    if extra_channels:
                        raise ValueError(f'Conda channels {extra_channels} are not supported by galaxy instance')
                else:
                    logger.warning('Conda channels are not defined in evironment file.')
                self.env_dict['channels'] = available_channels
                self.env_dict['dependencies'].extend(extra_req)
            else:
                self.env_dict = boilerplate_env_dict
        else:
            self.env_dict = boilerplate_env_dict

        match_spec = re.compile(r'^(?P<pac>[^=<> ]+)')
        self._direct_dependencies = []

        pip_reqs = []
        pip_dep_idx = None
        for i, dep in enumerate(self.env_dict['dependencies']):
            if isinstance(dep, dict) and list(dep.keys()) == ['pip']:
                for pipdep in dep['pip']:
                    parsed = self._parse_requirements_line(pipdep)
                    if parsed is not None:
                        pip_reqs.append(parsed)
                pip_dep_idx = i
            elif isinstance(dep, str):
                m = match_spec.match(dep)
                if m is None:
                    raise RuntimeError(f"Unable to parse conda dependency: {dep}")
                self._direct_dependencies.append((m.group('pac'), '', 0, ''))
            else:
                raise RuntimeError(f"Uexpected item in dependencies: {dep}")

        if pip_dep_idx is not None:
            del self.env_dict['dependencies'][pip_dep_idx]

        if requirements_txt is not None or pip_reqs:
            if requirements_txt is not None:
                pip_reqs += self._parse_requirements_txt(requirements_txt)

            channels_cl = []
            for ch in self.env_dict['channels']:
                channels_cl.append('-c')
                channels_cl.append(ch)

            for req in pip_reqs:
                if req[2] == 2:
                    self._direct_dependencies.append(req)
                    continue              
                run_cmd = [self.micromamba, 'search', '--json']
                run_cmd.extend(channels_cl)
                run_cmd.append(req[0]+req[1])

                search_res = sp.run(run_cmd, check=True, capture_output=True, text=True)
                search_json = json.loads(search_res.stdout)
                if search_json['result']['pkgs']:
                    self._direct_dependencies.append(req)
                    self.env_dict['dependencies'].append(req[0]+req[1])
                else:
                    logger.warning(f'Dependency {req[0]} not found in conda channels.')
                    self._direct_dependencies.append((req[0], req[1], 2, req[3]))

        if self.env_dict["dependencies"]:
            with open(self.fullenv_file_path, 'w') as fd:
                yaml.dump(self.env_dict, fd)

            resolved_env = self._resolve_environment_yml()
        else:
            resolved_env = {}

        self.final_dependencies = {}
        for dep in self._direct_dependencies:
            if dep[2] == 2:
                self.final_dependencies[dep[0]] = (dep[1], dep[2], dep[3])
            elif dep[0] in self.final_dependencies.keys():
                continue
            else:
                self.final_dependencies[dep[0]] = (resolved_env[dep[0]], dep[2], dep[3])

    def _resolve_environment_yml(self):

        with open(self.fullenv_file_path) as fd:
            logger.info(f'Will resolve environment:\n\n{fd.read()}')

        run_command = [str(self.micromamba),
                    'env', 'create',
                    '-n', '__temp_env_name',
                    '--dry-run',
                    '--json',
                    '-f', str(self.fullenv_file_path)]
        run_proc = sp.run(run_command, capture_output=True, check=True, text=True)
        resolved_env = json.loads(run_proc.stdout)['actions']['FETCH']
        resolved_env = {x['name']: x['version'] for x in resolved_env}

        return resolved_env

    def to_xml_tree(self):
        reqs_elements = []
        for name, det in self.final_dependencies.items():
            if det[1] == 2:
                reqs_elements.append(ET.Comment(
                    (f"Requirements string '{det[2]}' can't be converted automatically. "
                     "Please add the galaxy/conda requirement manually or modify the requirements file!")))
            else:
                reqs_elements.append(ET.Element('requirement',
                                                type='package',
                                                version = det[0]))
                reqs_elements[-1].text = name

        return reqs_elements

    @staticmethod
    def _parse_requirements_line(line: str) -> tuple | None:
        match_spec = re.compile(
            r"^(?P<pac>[A-Z0-9][A-Z0-9._-]*[A-Z0-9])\s*(?:\[.*\])?\s*(?P<eq>[~=]{0,2})(?P<uneq>[<>]?=?)\s*(?P<ver>[0-9.\*]*)",
            re.I,
        )
        match_from_url = re.compile(
            r"^(?P<pac>[A-Z0-9][A-Z0-9._-]*[A-Z0-9])\s*@(?P<path>.*)", re.I
        )

        if line.startswith("#") or re.match(r"^\s*$", line):
            return
        elif line.startswith("git+"):
            logger.warning("Dependency from git repo is not supported: %s", line)
            return (line, "", 2, line)
        elif match_from_url.match(line):
            logger.warning("Dependency from url is not supported %s", line)
            return (line, "", 2, line)
        else:
            m = match_spec.match(line)
            if m is None:
                logger.warning("Dependency spec not recognised for %s", line)
                return (line, "", 2, line)
            if m.group("ver"):
                ver = (
                    m.group("uneq") + m.group("ver")
                    if m.group("uneq")
                    else m.group("eq") + m.group("ver")
                )
            else:
                ver = ""
            return (m.group("pac"), ver, 1, line)

    def _parse_requirements_txt(self, filepath: str | os.PathLike) -> list[tuple]:

        with open(filepath, "r") as fd:
            reqs_str_list = []
            for line in fd:
                parsed = self._parse_requirements_line(line)
                if parsed is not None:
                    reqs_str_list.append(parsed)

        return reqs_str_list

    @staticmethod
    def _get_micromamba_binary():
        micromamba_dir = os.path.join(
            os.environ.get('HOME', os.getcwd()),
            '.local',
            'bin'
            )
        os.makedirs(micromamba_dir, exist_ok=True)
        mambapath = os.path.join(micromamba_dir, 'micromamba')

        if not os.path.isfile(mambapath):
            release_url="https://github.com/mamba-org/micromamba-releases/releases/download/1.5.10-0/micromamba-linux-64"
            res = requests.get(release_url, allow_redirects=True)
            with open(mambapath, 'wb') as fd:
                fd.write(res.content)

        if not os.access(mambapath, os.X_OK):
            os.chmod(mambapath, 0o744)

        return mambapath

def _split_bibfile(filepath):
    # parse bibfile and return only entries (no preamble/comments/strings) as list of strings
    biblib = bib.parse_file(filepath)
    
    out = []
    for ent in biblib.entries:
        doikey = [x.key for x in ent.fields if x.value and x.key in ['doi', 'Doi']]
        if doikey:
            out.append(('doi', [x.value for x in ent.fields if x.key in doikey][0]))
        else:
            tmplib = bib.Library()
            tmplib.add(ent)
            outstr = bib.write_string(tmplib)
            outstr = outstr.replace('\n', '\n\t\t')
            if outstr.endswith('\n\t\t'):
                outstr = outstr[:-3]
            out.append(('bibtex', outstr))
    return out

def _read_help_file(filepath):
    if filepath.endswith('.rst'):
        with open(filepath, 'r') as fd:
            help_text = fd.read()
    elif filepath.endswith('.md'):
        # TODO: test and adapt formatting arguments
        help_text = pypandoc.convert_file(filepath, 'rst')
    else:
        raise NotImplementedError('Unknown help file extension.')
    return help_text

def _test_data_location(
        repo_dir: str | os.PathLike, 
        tool_dir: str | os.PathLike, 
        par_value, 
        base_url=None, 
        name=None) -> tuple[str | None, str|None]:
    # assumes default_value is always a relative path (the case in MMODA)
    
    location = None
    value = None

    testdata = os.path.join(tool_dir, 'test-data')
    
    # for FileReference if default is URL
    if validators.url(par_value):
        location = par_value

    else:
        # check it's actually a file in repo
        
        try:
            abspath = os.path.join(repo_dir, par_value) # type: ignore
        except TypeError:
            abspath = None

        if abspath is not None and os.path.isfile(abspath):
            if base_url is None or os.path.getsize(abspath) < 1048576 :
                value = os.path.basename(par_value) # type: ignore
                os.makedirs(testdata, exist_ok=True)
                shutil.copy(abspath, os.path.join(testdata, value))
            else:
                # TODO: support the case of nb not in repo root
                location = os.path.join(base_url, os.path.normpath(par_value)) # type: ignore
        else:
            # it's a json value
            os.makedirs(testdata, exist_ok=True)
            value = f'{name}.json'
            with open(os.path.join(testdata, value), 'w') as fd:
                json.dump(par_value, fd)

    return value, location

def to_galaxy(input_path, 
              toolname, 
              out_dir, 
              tool_id = None,
              tool_version = '0.1.0+galaxy0',
              requirements_file = None, 
              conda_environment_file = None, 
              citations_bibfile = None,
              help_file = None,
              available_channels = ['default', 'conda-forge'],
              ontology_path = oda_ontology_path,
              test_data_baseurl = None
              ):
    """
    Converts a Jupyter notebook or a directory of notebooks into a Galaxy tool.

    Parameters:
    - input_path (str): Path to the notebook file or a directory containing notebooks.
    - toolname (str): Name of the Galaxy tool to be generated.
    - out_dir (str): Directory where the generated Galaxy tool files will be saved.
    - tool_id (str, optional): Unique identifier for the Galaxy tool. Defaults to a sanitized version of `toolname`.
    - tool_version (str, optional): Version of the Galaxy tool. Defaults to '0.1.0+galaxy0'.
    - requirements_file (str, optional): Path to a `requirements.txt` file specifying Python dependencies.
        Default behavior: If omitted, no additional Python dependencies are included unless specified in the Conda environment file.
    - conda_environment_file (str, optional): Path to a Conda environment YAML file specifying dependencies.
        Default behavior: If omitted, only the default dependencies required by the notebook are included.
    - citations_bibfile (str, optional): Path to a BibTeX file containing citations for the tool.
        Default behavior: If omitted, no citations are included in the Galaxy tool.
    - help_file (str, optional): Path to a help file (Markdown or reStructuredText) to include in the Galaxy tool.
        Default behavior: If omitted, no help documentation is added to the Galaxy tool.
    - available_channels (list[str], optional): List of Conda channels to use for resolving dependencies.
        Default behavior: If omitted, the default channels ['default', 'conda-forge'] are used.
    - ontology_path (str, optional): Path to an ontology file for parameter and output metadata.
        Default behavior: If omitted, the default ontology `oda_ontology_path` is used.
    - test_data_baseurl (str, optional): Base URL for test data, used to resolve input parameters that reference external files.
        Default behavior: If omitted, test data is copied locally into the `test-data` directory of the Galaxy tool.

    Outputs:
    - A Galaxy tool XML file (`<toolname>.xml`) is generated in the specified `out_dir`.
    - Python scripts corresponding to the notebook(s) are generated in the `out_dir`.
    - Additional files, such as test data and metadata, are created as needed.
    """
    ontology = Ontology(ontology_path) if ontology_path != oda_ontology_path else nba_ontology_global_var

    os.makedirs(out_dir, exist_ok=True)
    
    nbas = find_notebooks(input_path)
    
    if tool_id is not None:
        tid = tool_id
    else:
        tid = re.sub(r'[^a-z0-9_]', '_', toolname.lower())
    
    tool_root = ET.Element('tool',
                        id=tid,
                        name=toolname,
                        version=tool_version, 
                        profile='24.0')

    reqs = ET.SubElement(tool_root, 'requirements')
    extra_req = global_req
    # will be populated after script generation to check if ipython is needed
                
    comm = ET.SubElement(tool_root, 'command', detect_errors='exit_code')
    python_binary = 'python'
    # the same to decide python/ipython

    env = ET.SubElement(tool_root, 'environment_variables')    
    conf = ET.SubElement(tool_root, 'configfiles')
    conf.append(ET.Element('inputs', name='inputs', filename='inputs.json', data_style='paths'))

    bd_env_v = ET.SubElement(env, 'environment_variable', attrib={'name': 'BASEDIR'})
    bd_env_v.text = "$__tool_directory__"
    td_env_v = ET.SubElement(env, 'environment_variable', attrib={'name': 'GALAXY_TOOL_DIR'})
    td_env_v.text = "$__tool_directory__"

    inps = ET.SubElement(tool_root, 'inputs')
    outps = ET.SubElement(tool_root, 'outputs')
    tests = ET.SubElement(tool_root, 'tests')
    
    
    if len(nbas) > 1:
        dprod_cond = ET.SubElement(inps, 'conditional', name='C_data_product_')
        dprod_sel = ET.SubElement(dprod_cond, 'param', name="DPselector_", type="select", label = "Data Product")
        sflag = True
        for name in nbas.keys():
            opt = ET.SubElement(dprod_sel, 'option', value=name, selected='true' if sflag else 'false')
            opt.text = name
            sflag = False
            
    for nb_name, nba in nbas.items():
        inputs = nba.input_parameters
        outputs = nba.extract_output_declarations()

        galaxy_pars = []
        galaxy_otps = []
        
        default_test = ET.SubElement(tests, 'test', expect_num_outputs=str(len(outputs)))
        
        if len(nbas) > 1:
            when = ET.SubElement(dprod_cond, 'when', value=nb_name) # type: ignore
            test_par_root = ET.SubElement(default_test, 'conditional', name='C_data_product_')
            test_par_root.append(ET.Element('param', name='DPselector_', value=nb_name))
        else:
            when = inps
            test_par_root = default_test

        for pv in inputs.values():
            galaxy_par = GalaxyParameter.from_inspect(pv, ontology=ontology)
            galaxy_pars.append(galaxy_par)
            when.append(galaxy_par.to_xml_tree())
            if galaxy_par.partype != 'data':
                if galaxy_par.default_value is not None:
                    test_par_root.append(ET.Element('param', name=galaxy_par.name, value=str(galaxy_par.default_value)))
            elif galaxy_par.original_value is None:
                pass  # for optional file input that's None by default, just don't set in test
            else:
                repo_dir = input_path if os.path.isdir(input_path) else os.path.dirname(os.path.realpath(input_path))
                value, location = _test_data_location(repo_dir, 
                                                      out_dir, 
                                                      galaxy_par.original_value,
                                                      test_data_baseurl,
                                                      name=galaxy_par.name)
                if value:
                    test_par_root.append(ET.Element('param', name=galaxy_par.name, value=value))
                elif location:
                    test_par_root.append(ET.Element('param', name=galaxy_par.name, location=location))
                else:
                    raise RuntimeError(f"Unable to build test data location for parameter '{galaxy_par.name}'")

                
        for outv in outputs.values():
            outp = GalaxyOutput.from_inspect(outv, ontology=ontology, dprod=nb_name)
            galaxy_otps.append(outp)
            outp_tree = outp.to_xml_tree()
            if len(nbas) > 1:
                fltr = ET.SubElement(outp_tree, 'filter')
                fltr.text = f"C_data_product_['DPselector_'] == '{nb_name}'"
            outps.append(outp_tree)

        script_str = _nb2script(nba, inputs=galaxy_pars, outputs=galaxy_otps)
        if 'get_ipython()' in script_str or re.search(r'^\s*display\(', script_str):
            python_binary = 'ipython'
            if 'ipython' not in extra_req:
                extra_req.append('ipython')
        
        with open(os.path.join(out_dir, f'{nb_name}.py'), 'w') as fd:
            fd.write(script_str)

        assert_stdout = ET.SubElement(default_test, 'assert_stdout')
        assert_stdout.append(ET.Element('has_text', text=_success_text))

    reqs.extend(Requirements(available_channels=available_channels, 
                             conda_env_yml=conda_environment_file,
                             requirements_txt=requirements_file,
                             extra_req=extra_req).to_xml_tree())
    if len(nbas) > 1:
        comm.text = python_binary + " '$__tool_directory__/${C_data_product_.DPselector_}.py'" 
    else:
        comm.text = f"{python_binary} '$__tool_directory__/{list(nbas.keys())[0]}.py'"
    # NOTE: CDATA if needed https://gist.github.com/zlalanne/5711847

    if help_file is not None:    
        help_block = ET.SubElement(tool_root, 'help')
        help_text = _read_help_file(help_file)
        help_block.text = help_text
    
    if citations_bibfile is not None:
        citats = ET.SubElement(tool_root, 'citations')
        bibentries = _split_bibfile(citations_bibfile)
        for entry in bibentries:
            citate = ET.SubElement(citats, 'citation', type=entry[0])
            citate.text = entry[1]

    tree = ET.ElementTree(tool_root)
    ET.indent(tree)
    
    out_xml_path = os.path.join(out_dir, f"{tid}.xml")
    tree.write(out_xml_path)

# %%
def main():
    parser = argparse.ArgumentParser(
        description=(
            'Convert a Jupyter notebook file or a directory containing notebooks into a Galaxy tool '
            'by generating the required XML configuration, scripts, and other supporting files.'
        )
    )
    parser.add_argument(
        'notebook_source', 
        type=str, 
        help='Path to the Jupyter notebook file or a directory containing notebooks to be converted.'
    )
    parser.add_argument(
        'outdir', 
        type=str, 
        help='Directory where the generated Galaxy tool files will be saved.'
    )
    parser.add_argument(
        '--name', 
        type=str, 
        default='example', 
        help='Name of the Galaxy tool.'
    )
    parser.add_argument(
        '--tool_version', 
        type=str, 
        default='0.1.0+galaxy0', 
        help='Version of the Galaxy tool. Defaults to "0.1.0+galaxy0".'
    )
    parser.add_argument(
        '--requirements_txt', 
        required=False, 
        help=(
            'Path to a requirements.txt file specifying Python dependencies. '
            'If omitted, no additional Python dependencies are included unless specified in the Conda environment file.'
            'If package name from this file is also resolvable by conda, it will be included to the list of tool dependencies, ' 
            'otherwise a comment will be added to the tool xml.'
        )
    )
    parser.add_argument(
        '--environment_yml', 
        required=False, 
        help=(
            'Path to a Conda environment YAML file specifying dependencies. '
            'If omitted, no additional Python dependencies are included unless specified in the requirements file.'
        )
    )
    parser.add_argument(
        '--ontology_path', 
        required=False, 
        help=(
            'Path to an ontology file for parameter and output metadata. '
            'If omitted, the default MMODA ontology is used.'
        )
    )
    parser.add_argument(
        '--citations_bibfile', 
        required=False, 
        help=(
            'Path to a BibTeX file containing citations for the tool. '
            'If omitted, no citations are included in the Galaxy tool.'
        )
    )
    parser.add_argument(
        '--help_file', 
        required=False, 
        help=(
            'Path to a help file (Markdown or reStructuredText) to include in the Galaxy tool. '
            'If omitted, no help documentation is added to the Galaxy tool.'
        )
    )
    parser.add_argument(
        '--test_data_baseurl', 
        required=False, 
        help=(
            'Base URL for test data, used to resolve input parameters that reference external files. '
            'If omitted, test data is copied locally into the "test-data" directory of the Galaxy tool.'
        )
    )
    parser.add_argument(
        '--conda_channels', 
        required=False, 
        default='default,conda-forge', 
        help=(
            'Comma-separated list of Conda channels to use for resolving dependencies. '
            'Defaults to "default,conda-forge".'
        )
    )
    args = parser.parse_args()
    
    input_nb = args.notebook_source
    output_dir = args.outdir
    toolname = args.name
    requirements_txt = args.requirements_txt
    environment_yml = args.environment_yml
    tool_version = args.tool_version
    ontology_path = args.ontology_path
    ontology_path = oda_ontology_path
    bibfile = args.citations_bibfile
    help_file = args.help_file
    test_data_baseurl = args.test_data_baseurl
    available_channels = args.conda_channels.split(',')
    
    
    os.makedirs(output_dir, exist_ok=True)
    to_galaxy(input_nb, 
              toolname, 
              output_dir, 
              tool_version=tool_version,
              requirements_file=requirements_txt, 
              conda_environment_file=environment_yml,
              citations_bibfile=bibfile,
              help_file=help_file,
              ontology_path=ontology_path,
              test_data_baseurl=test_data_baseurl,
              available_channels=available_channels)

if __name__ == '__main__':
    main()
