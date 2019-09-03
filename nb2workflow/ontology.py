import io
import logging
import argparse

import nb2workflow.nbadapter as nbadapter

logger = logging.getLogger(__name__)

import rdflib
import owlready2
    
xsd = owlready2.get_ontology("https://www.w3.org/2001/XMLSchema#").load()
kees = owlready2.get_ontology("http://linkeddata.center/kees/v1#").load()

fno = owlready2.get_ontology("http://ontology.odahub.io/function.rdf").load()
fno.base_iri="https://w3id.org/function/ontology#"

def get_dda():
    return owlready2.get_ontology("http://ddahub.io/ontology/analysis#")


def to_xsd_type(p):
    out_type='string'

    if issubclass(p['python_type'],int):
        out_type='integer'

    if issubclass(p['python_type'],float):
        out_type='double'

    if issubclass(p['python_type'],str):
        out_type='string'

    logger.debug("owl type cast from %s to %s",p,repr(out_type))
    
    return p.get('owl_type',"http://www.w3.org/2001/XMLSchema#"+out_type)



def function_semantic_signature(dda, function_name, parameters, output):
    with dda:
        parameter_attrs={}
        for pn,pv in parameters.items():
            p_cls = type(
                        pn,(fno.Parameter,),
                        {}
                    )
            parameter_attrs[pn] = p_cls

            s,p,o = (dda.graph.abbreviate(p_cls.iri), 
                     dda.graph.abbreviate(fno.type.iri),
                     dda.graph.abbreviate(to_xsd_type(pv)))

            if len(dda.get_triples(s,p,o)) == 0:
                dda.add_triple(s,p,o)


        cls = type(function_name,(dda.WebDataAnalysis,),
                    dict(expects=parameter_attrs.values())
                )
            
        cls().url = "http://api.odahub.io/"+function_name

    

def service_semantic_signature(nbas, format="rdfxml"):
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




def nb2rdf(notebook_fn, rdf_fn):
    nba = nbadapter.NotebookAdapter(notebook_fn)

    with open(rdf_fn, "wb") as f:
        rdf = service_semantic_signature(dict(local=nba))

        G = rdflib.Graph()
        G.parse(data=rdf, format="xml")
        rdf = G.serialize(format="turtle")

        logging.getLogger().info("rdf: "+rdf.decode())
        f.write(rdf)


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('notebook', metavar='notebook', type=str)
    parser.add_argument('rdf', metavar='rdf', type=str)
    parser.add_argument('--publish', metavar='upstream-url', type=str, default=None)
    parser.add_argument('--publish-as', metavar='published url', type=str, default=None)
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

    nb2rdf(args.notebook, args.rdf)


if __name__ == "__main__":
    main()
