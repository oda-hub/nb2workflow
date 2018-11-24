import logging

logger = logging.getLogger(__name__)

import owlready2 

def function_semantic_signature(function_name, parameters, output):
    pass

def service_semantic_signature(nbas):
    r=[]
    for target, nba in nbas.items():
        r.append(function_semantic_signature(target,
                                    parameters=nba.extract_parameters(),
                                    output=nba.extract_output_declarations()))
    return r
