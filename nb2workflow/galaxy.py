#%%
import xml.etree.ElementTree as ET
import os
import re
from textwrap import dedent
import argparse
import logging

from cdci_data_analysis.analysis.ontology import Ontology
# TODO: ontology module must be separated from the dispatcher
from nb2workflow.nbadapter import NotebookAdapter

import nbformat
from nbconvert.exporters import ScriptExporter

logger = logging.getLogger()

#%%
onto = Ontology('http://odahub.io/ontology/ontology.ttl')

#%%

class GalaxyParameter:
    def __init__(self, 
                 name, 
                 python_type,
                 description=None,
                 default_value=None, 
                 min_value=None, 
                 max_value=None, 
                 allowed_values=None):
        
        partype_lookup = {str: 'text', 
                    bool: 'boolean',
                    float: 'float',
                    int: 'integer'} 
        
        partype = partype_lookup[python_type]
        
        if allowed_values is not None:
            partype = 'select'
        
        self.name = name
        self.partype = partype
        self.description=description
        self.default_value = default_value
        self.min_value = min_value
        self.max_value = max_value
        self.allowed_values = allowed_values
                
    
    @classmethod
    def from_owl(cls, name, owl_uri, python_type, extra_ttl=None, default_value=None):
        if extra_ttl is not None:
            onto.parse_extra_triples(extra_ttl)
        parameter_hierarchy = onto.get_parameter_hierarchy(owl_uri)
        par_format = onto.get_parameter_format(owl_uri)
        par_unit = onto.get_parameter_unit(owl_uri)
        min_value, max_value = onto.get_limits(owl_uri)
        allowed_values = onto.get_allowed_values(owl_uri)

        description = f"type: {owl_uri}"
        if par_format is not None:
            description += f"; format: {par_format}"
        if par_unit is not None:
            description += f"; units: {par_unit}"
        
        return cls(name, 
                   python_type, #TODO: type is fully defined by owl_type. Use it instead?
                   description=description,
                   default_value=default_value, 
                   min_value=min_value,
                   max_value=max_value,
                   allowed_values=allowed_values)

    def to_xml_tree(self):
        attrs = {'name': self.name,
                 'type': self.partype}
        if self.default_value is not None and self.partype != 'select':
            attrs['value'] = str(self.default_value)
        if self.description is not None:
            attrs['label'] = self.description
            
        if self.min_value is not None:
            attrs['min'] = str(self.min_value)
        if self.max_value is not None:
            attrs['max'] = str(self.max_value)
        
        element = ET.Element('param',
                             **attrs)
        
        if self.allowed_values is not None:
            for val in self.allowed_values:
                attrs = {'value': str(val)}
                if val == self.default_value:
                    attrs['selected'] = 'true'
                option = ET.SubElement(element, 'option', *attrs)
                option.text = str(val)
        
        # TODO: do we need additional validation?
        
        return element
    
def _nb2script(nba):
    input_nb = nba.notebook_fn
    mynb = nbformat.read(input_nb, as_version=4)
    outputs = nba.extract_output_declarations()

    inject_pos = 0
    for pos, cell in enumerate(mynb.cells):
        if 'parameters' in cell['metadata'].get('tags', []):
            inject_pos = pos + 1
            break
    # NOTE: validation of args is external
    inject_read = nbformat.v4.new_code_cell(
        dedent("""
            import json
            import sys
            with open(sys.argv[1], 'r') as fd:
                inp_dic = json.load(fd)
            for vn, vv in inp_dic.items():
                globals()[vn] = type(globals()[vn])(vv)
            """))
    inject_read.metadata['tags'] = ['injected-input']
    mynb.cells.insert(inject_pos, inject_read)

    outp_code = dedent("""
                    from json_tricks import dump as e_dump
                    output_dic = {}
                    """)
    for vn in outputs.keys():
        outp_code += f"output_dic['{vn}']={vn}\n"       
    outp_code += "e_dump(output_dic, sys.argv[2])\n"

    inject_write = nbformat.v4.new_code_cell(outp_code)
    inject_write.metadata['tags'] = ['injected-output']
    mynb.cells.append(inject_write)
    
    exporter = ScriptExporter()
    script, resources = exporter.from_notebook_node(mynb)
    
    return script

# TODO: several notebooks
def to_galaxy(input_nb, toolname, requirements_path, out_dir):
    nba = NotebookAdapter(input_nb)
    inputs = nba.input_parameters

    script_str = _nb2script(nba)
    
    with open(os.path.join(out_dir, 'script.py'), 'w') as fd:
        fd.write(script_str)

    tool_root = ET.Element('tool',
                        id=toolname.replace(' ', '_'),
                        name=toolname,
                        version='0.1.0+galaxy0', #TODO:
                        profile='23.0')

    reqs = ET.SubElement(tool_root, 'requirements')

    for greq in ['json_tricks']:
        req = ET.SubElement(reqs, 
                            'requirement', 
                            type='package'
                            )
        req.text = greq
    if requirements_path is not None:
        with open(requirements_path, 'r') as fd:
            for line in fd:
                # TODO: this is just an example as galaxy doesn't use pip for resolving
                #       we still want to find correspondance in conda-forge and also use envitronment.yml
                #       also package version (does galaxy allow gt/lt?)
                m = re.match(r'[^#(git)]\S+', line)
                if m is not None:
                    req = ET.SubElement(reqs, 
                                        'requirement', 
                                        type='package'
                                        )
                    req.text = m.group(0)
                
    comm = ET.SubElement(tool_root, 'command', detect_errors='exit_code')
    comm.text = "python '$__tool_directory__/script.py' inputs.json '$output'"
    # NOTE: CDATA if needed https://gist.github.com/zlalanne/5711847

    conf = ET.SubElement(tool_root, 'configfiles')
    inp = ET.SubElement(conf, 'inputs', name='inputs', filename='inputs.json')
    
    inps = ET.SubElement(tool_root, 'inputs')
    for pn, pv in inputs.items():
        galaxy_par = GalaxyParameter.from_owl(pn,
                                            pv['owl_type'],
                                            pv['python_type'],
                                            extra_ttl=pv['extra_ttl'],
                                            default_value=pv['default_value']
                                            )
        inps.append(galaxy_par.to_xml_tree())

    outps = ET.SubElement(tool_root, 'outputs')
    outp = ET.SubElement(outps, 'data', name='output', format='json')

    #TODO: tests

    help_block = ET.SubElement(tool_root, 'help')
    help_block.text = 'help me!' # TODO:

    citats = ET.SubElement(tool_root, 'citations')
    citate = ET.SubElement(citats, 'citation', type='doi')
    citate.text = '10.5281/zenodo.6299481' # TODO:

    tree = ET.ElementTree(tool_root)
    ET.indent(tree)
    
    out_xml_path = os.path.join(out_dir, f"{toolname}.xml")
    tree.write(out_xml_path)

# %%
def main():
    parser = argparse.ArgumentParser(
        description='Convert python notebook to galaxy tool'
        )
    parser.add_argument('notebook', type=str)
    parser.add_argument('outdir', type=str)
    parser.add_argument('--name', type=str, default='example')
    parser.add_argument('--requirements_path', required=False)
    args = parser.parse_args()
    
    input_nb = args.notebook
    output_dir = args.outdir
    toolname = args.name
    requirements_path = args.requirements_path   
    
    to_galaxy(input_nb, toolname, requirements_path, output_dir)

if __name__ == '__main__':
    main()

