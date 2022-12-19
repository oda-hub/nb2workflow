import io
import logging
import argparse
import re
from typing import Optional
import uuid

import nb2workflow.nbadapter as nbadapter

logger = logging.getLogger(__name__)

import requests
import rdflib

try:
    import odakb.sparql
except Exception:
    logger.error("some error")


oda_ontology_prefix = "http://odahub.io/ontology#"    
   

try:
    import owlready2
        
    xsd = owlready2.get_ontology("https://www.w3.org/2001/XMLSchema#").load()
    kees = owlready2.get_ontology("http://linkeddata.center/kees/v1#").load()

    G = rdflib.Graph()
    open("function.xml", "w").write(G.load(io.StringIO(requests.get("https://raw.githubusercontent.com/FnOio/fnoio.github.io/master/ontology/0.4.1/function.ttl").text), format="turtle").serialize(format="xml"))

    fno = owlready2.get_ontology("function.xml").load()
    # fno.base_iri="https://w3id.org/function/ontology#"

    odaworkflow = owlready2.get_ontology("http://odahub.io/ontology/workflow#")
except Exception as e:
    logger.warning('unable to import owlready2: %s', e)
    owlready2=None

def get_dda():
    if owlready2 is None:
        return

    return owlready2.get_ontology("http://ddahub.io/ontology/analysis#")


def to_xsd_type(p):
    # if owlready2 is None:
    #     return

    out_type='string'

    if issubclass(p['python_type'],int):
        out_type='integer'

    if issubclass(p['python_type'],float):
        out_type='double'

    if issubclass(p['python_type'],str):
        out_type='string'

    logger.debug("owl type cast from %s to %s",p,repr(out_type))
    
    return p.get('owl_type', "http://www.w3.org/2001/XMLSchema#"+out_type)


#TODO: return owl option as an option

def function_semantic_signature(function_name, location, parameters, output, domains):
    G = rdflib.Graph()
    
    wfl = rdflib.URIRef(f'http://odahub.io/workflows#{function_name}')    
    
    oda_ns = rdflib.Namespace('http://odahub.io/ontology#')
    rdf_ns = rdflib.Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
    wfl_p_ns = rdflib.Namespace(f'http://odahub.io/workflows/{function_name}/parameter_bindings#')
    
    G.bind('oda', oda_ns)
    G.bind('rdfs', rdf_ns)
    # G.bind('wfl', wfl_p_ns)

    G.add((wfl, rdf_ns['type'], oda_ns['workflow']))
    G.add((wfl, oda_ns['location'], rdflib.Literal(location)))

    for pn, pv in parameters.items():
        logger.info('function_semantic_signature parameter pn=%s pv=%s', pn, pv)
        p_uri = wfl_p_ns[pn]
        logger.info('function_semantic_signature parameter p_uri=%s', p_uri)
        G.add((p_uri, rdf_ns['type'], rdflib.URIRef(to_xsd_type(pv))))
        G.add((wfl, oda_ns['expects'], p_uri))

    if domains is not None:
        for domain in domains:
            G.add((wfl, oda_ns['domain'], oda_ns[domain[0]]))
                    
    return G


def service_semantic_signature(nbas, format="xml", domains=None) -> str:
    G = rdflib.Graph()

    for target, nba in nbas.items():
        logger.info("target: %s nba: %s", target, nba)
        S_G = function_semantic_signature(
                                    nba.unique_name,
                                    location=nba.notebook_origin,
                                    parameters=nba.extract_parameters(),
                                    output=nba.extract_output_declarations(),
                                    domains=domains
                                    )

        for t in S_G:
            G.add(t)


    rdf_str = G.serialize(format=format)

    logger.debug(rdf_str)
    
    return rdf_str




def service_semantic_signature_owl(nbas, format="rdfxml"):
    if owlready2 is None:
        return
    dda = get_dda()
    dda.graph.destroy()
    dda = get_dda()

    with dda:
        class DataAnalysis(fno.Function):
            pass
        
        class WebDataAnalysis(DataAnalysis):
            pass
        
    r=[]
    for target, nba in nbas.items():
        r.append(function_semantic_signature(dda,target,
                                    parameters=nba.extract_parameters(),
                                    output=nba.extract_output_declarations()))

    f=io.BytesIO()
    dda.save(f, format=format)
    owl_str=f.getvalue().decode("unicode_escape")
    owl_str = bytes(owl_str, "utf-8").decode("unicode_escape")

    logger.debug(owl_str)
    
    return str(owl_str)


# TODO: register this function as versioned rdf-generating workflow
def understand_comment_references(comment, inline=True):
    comment = comment.strip()

    logger.debug("treating %s comment %s", "inline" if inline else "free-line", comment)

    comment = re.sub(r"\b("+oda_ontology_prefix+r".*?)(?:\s+|$)", r"<\1>", comment)
    
    a = rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
    param_uri = rdflib.URIRef(f"{oda_ontology_prefix}{uuid.uuid1().hex}")
    combined_ttl = f"@prefix oda: <{oda_ontology_prefix}> . {param_uri.n3()} a {comment} ."
    logger.info("combined turtle: %s", combined_ttl)

    G = rdflib.Graph()
    G.bind("oda", rdflib.Namespace(oda_ontology_prefix))        

    G.parse(data=combined_ttl, 
            format="turtle")
    logger.info("interpreted turtle: %s", G.serialize(format="turtle"))

    
    owl_types = list(G.objects(param_uri, a))
    predicate_objects = list(G.predicate_objects(param_uri))
    
    logger.info("types: %s", owl_types)

    for p, o in predicate_objects:
        logger.info("extra predicate %s: %s", p, o)
    
    if len(owl_types) == 1 and len(predicate_objects) == 1:
        owl_type = owl_types[0]

        G.remove((param_uri, a, owl_type))

        extra_ttl = G.serialize(format="turtle")

    elif len(predicate_objects) > 1:
        factors = []

        for p, o in sorted(predicate_objects):
            for t in p, o:
                t = t.n3()
                for common_ns in ["http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                                  "http://www.w3.org/2001/XMLSchema#",
                                  oda_ontology_prefix]:
                    t = t.replace(common_ns, "")

                t = re.sub("[^a-zA-Z0-9_]", "", t)
                
                if t!="":
                    factors.append(t)
            
        # it does not matter exactly how this is formatted as long as it is unique
        # it is good that it is readable
        merged_type = oda_ontology_prefix + "_".join(factors)
        
        logger.info("merged type %s", merged_type)
        owl_type = merged_type

        for p, o in sorted(predicate_objects):
            G.remove((param_uri, p, o))
            G.add((rdflib.URIRef(merged_type), p, o))

        extra_ttl = G.serialize(format="turtle")
    else:
        raise NotImplementedError("no semantic annotation found")

    return dict(
        owl_type = str(owl_type),
        extra_ttl = extra_ttl,
    )



def nb2rdf(notebook_fn: str, domains: Optional[list]=None) -> str:
    nba = nbadapter.NotebookAdapter(notebook_fn)

    rdf = service_semantic_signature(dict(local=nba), format="turtle", domains=domains)
            
    logging.getLogger().info("rdf: %s", rdf)

    return rdf


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('--out-rdf', metavar='rdf', type=str)
    parser.add_argument('--domain', dest='domain', nargs="*", action='append')
    parser.add_argument('--publish', action="store_true")
    parser.add_argument('--debug', action="store_true")

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

    logger.error('domain %s', args)

    rdf = nb2rdf(args.notebook, domains=args.domain)

    if args.out_rdf:
        with open(args.out_rdf, "wt") as f:
            f.write(rdf)

    if args.publish:
        G = rdflib.Graph()
        G.parse(data=rdf)

        odakb.sparql.insert("\n".join([
            f"{s.n3()} {p.n3()} {o.n3()} ." for s, p, o in G
        ]))
    


if __name__ == "__main__":
    main()
