
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

# NOTE: to include into the base class when separated from dispatcher
class ModOntology(Ontology):
    def get_oda_label(self, param_uri):
        if param_uri.startswith("http"): param_uri = f"<{param_uri}>"

        query = "SELECT ?label WHERE {%s oda:label ?label}" % (param_uri)
        
        qres = self.g.query(query)
   
        if len(qres) == 0: return None
        
        label = " ".join([str(x[0]) for x in qres])
        
        return label

    def is_data_product(self, owl_uri, include_parameter_products=True):
        if owl_uri.startswith("http"): owl_uri = f"<{owl_uri}>"
        
        filt_param = 'MINUS{?cl rdfs:subClassOf* oda:ParameterProduct. }' if not include_parameter_products else ''
        query = dedent("""
                       SELECT (count(?cl) as ?count) WHERE {
                       VALUES ?cl { %s } 
                       ?cl rdfs:subClassOf* oda:DataProduct. 
                       %s
                       }
                       """ % (owl_uri, filt_param))
        qres = self.g.query(query)
        
        if int(list(qres)[0][0]) == 0: return False
        
        return True
        
               
# TODO: configurable
#ontology_path = 'http://odahub.io/ontology/ontology.ttl'
ontology_path = '/home/dsavchenko/Projects/MMODA/ontology/ontology.ttl'


global_req = []


class GalaxyParameter:
    def __init__(self, 
                 name, 
                 python_type,
                 description=None,
                 default_value=None, 
                 min_value=None, 
                 max_value=None, 
                 allowed_values=None):
        
        #TODO: type is fully defined by owl_type. Use it instead?  
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
    def from_inspect(cls, par_details):
        onto = ModOntology(ontology_path)

        owl_uri = par_details['owl_type']
        
        if par_details.get('extra_ttl') is not None:
            onto.parse_extra_triples(par_details['extra_ttl'])
        par_format = onto.get_parameter_format(owl_uri)
        par_unit = onto.get_parameter_unit(owl_uri)
        min_value, max_value = onto.get_limits(owl_uri)
        allowed_values = onto.get_allowed_values(owl_uri)
        label = onto.get_oda_label(owl_uri)
        
        description = label if label is not None else par_details['name']
        if par_format is not None:
            description += f" Format: {par_format}"
        if par_unit is not None:
            description += f" Units: {par_unit}"
        
        return cls(par_details['name'], 
                   par_details['python_type'], 
                   description=description,
                   default_value=par_details['default_value'], 
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
                option = ET.SubElement(element, 'option', **attrs)
                option.text = str(val)
        
        # TODO: do we need additional validation?
        
        return element

class GalaxyOutput:
    def __init__(self, name, is_oda):
        self.name = name
        self.dataname = f"out_{self.name}"
        self.is_oda = is_oda
        self.outfile_name = f"{name}_galaxy.output"
    
    @classmethod
    def from_inspect(cls, outp_details):
        onto = ModOntology(ontology_path)

        owl_uri = outp_details['owl_type']
        if outp_details['extra_ttl'] is not None:
            onto.parse_extra_triples(outp_details['extra_ttl'])
        
        if onto.is_data_product(owl_uri, include_parameter_products=False):
            is_oda = True
        else:
            is_oda = False
        
        return cls(outp_details['name'], is_oda)

    def to_xml_tree(self):
        attrs = {'label': "${tool.name} -> %s"%self.name,
                 'name': self.dataname,
                 #'auto_format': 'true', 
                 'format': 'auto',
                 'from_work_dir': self.outfile_name}
        
        element = ET.Element('data', **attrs)

        return element
    
        
            
def _nb2script(nba):
    input_nb = nba.notebook_fn
    mynb = nbformat.read(input_nb, as_version=4)
    outputs = nba.extract_output_declarations()
    
    inject_import = nbformat.v4.new_code_cell(
        dedent( """
                import json
                import os
                import shutil
                import sys
                
                try:
                    import numpy as np
                    _numpy_available = True
                except:
                    _numpy_available = False
                
                _galaxy_wd = os.getcwd()
                """))
    inject_import.metadata['tags'] = ['injected_import']
    mynb.cells.insert(0, inject_import)
    
    inject_pos = 0
    for pos, cell in enumerate(mynb.cells):
        if 'parameters' in cell['metadata'].get('tags', []):
            inject_pos = pos + 1
            break
    # NOTE: validation of args is external
    inject_read = nbformat.v4.new_code_cell(
        dedent("""
            with open(sys.argv[1], 'r') as fd:
                inp_dic = json.load(fd)
            for vn, vv in inp_dic.items():
                globals()[vn] = type(globals()[vn])(vv)
            """))
    inject_read.metadata['tags'] = ['injected-input']
    mynb.cells.insert(inject_pos, inject_read)
    
    outp_code = "_simple_outs, _oda_outs = [], []\n"
    outp_code += "_galaxy_meta_data = {}\n"
    
    for vn, vv in outputs.items():
        outp = GalaxyOutput.from_inspect(vv)
        if outp.is_oda:
            outp_code += f"_oda_outs.append(('{outp.dataname}', '{outp.outfile_name}', {vn}))\n"
        else:
            outp_code += f"_simple_outs.append(('{outp.dataname}', '{outp.outfile_name}', {vn}))\n"
    
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
                        json.dump(_outv.encode(), fd)
                    _galaxy_meta_data[_outn] = {'ext': 'json'}
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

    outp_code += dedent("""
                        with open(os.path.join(_galaxy_wd, 'galaxy.json'), 'w') as fd:
                            json.dump(_galaxy_meta_data, fd)
                        """)

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

    for greq in global_req:
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
                m = re.match(r'(?!#|git)\S+', line)
                if m is not None:
                    req = ET.SubElement(reqs, 
                                        'requirement', 
                                        type='package'
                                        )
                    req.text = m.group(0)
                
    comm = ET.SubElement(tool_root, 'command', detect_errors='exit_code')
    comm.text = "python '$__tool_directory__/script.py' inputs.json"
    # NOTE: CDATA if needed https://gist.github.com/zlalanne/5711847

    conf = ET.SubElement(tool_root, 'configfiles')
    inp = ET.SubElement(conf, 'inputs', name='inputs', filename='inputs.json')
    
    inps = ET.SubElement(tool_root, 'inputs')
    for pn, pv in inputs.items():
        galaxy_par = GalaxyParameter.from_inspect(pv)
        inps.append(galaxy_par.to_xml_tree())

    outps = ET.SubElement(tool_root, 'outputs')
    outputs = nba.extract_output_declarations()
    for outn, outv in outputs.items():
        outp = GalaxyOutput.from_inspect(outv)
        outps.append(outp.to_xml_tree())

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
