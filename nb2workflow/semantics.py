import re
import uuid
import logging
import rdflib

logger = logging.getLogger(__name__)

oda_ontology_prefix = "http://odahub.io/ontology#"    
oda = rdflib.Namespace(oda_ontology_prefix)
a = rdflib.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')


# TODO: register this function as versioned rdf-generating workflow
def understand_comment_references(comment, base_uri=None, fallback_type=None) -> dict:
    if base_uri is None:
        base_uri = rdflib.URIRef(f"{oda_ontology_prefix}{uuid.uuid1().hex}")
        deduce_type = True
    else:
        deduce_type = False            

    comment = comment.strip()    

    logger.debug('understand_comment_references: "%s"', comment)

    # this allows to use simplified syntax in some cases, e.g. when just a url alone is provided to indicate type
    comment = re.sub(rf"\b(http.*?)(?:\s|$)", r"<\1>", comment)

    logger.debug('preprocessed comment: "%s"', comment)
            
    parsed = None
    parse_failures = []

    variations = [
        f"@prefix oda: <{oda_ontology_prefix}> . {base_uri.n3()} a {comment} .",
        f"@prefix oda: <{oda_ontology_prefix}> . {base_uri.n3()} {comment} .",
    ]

    if fallback_type is not None:
        variations.append(f"@prefix oda: <{oda_ontology_prefix}> . {base_uri.n3()} a <{fallback_type}>; {comment} .")

    
    for variation in variations:
        try:
            parsed = parse_ttl(variation, base_uri, deduce_type)
            logger.info("this variation WAS parsed: %s to %s", variation, parsed)
        except (rdflib.plugins.parsers.notation3.BadSyntax, NotImplementedError) as e:
            logger.info("this variation could not be parsed: %s due to %s", variation, e)
            parse_failures.append([variation, e])

    if parsed is None:
        logger.info("all attempts to parse failed %s", parse_failures)
        return {
            "owl_type": fallback_type,
            "extra_ttl": None,
        }
    else:
        return parsed    



    
def parse_ttl(combined_ttl, param_uri, deduce_type=True):
    # here there is some simplification with respect to owl meaning of subclasses and their predicates
    logger.info("input combined turtle: %s", combined_ttl)

    G = rdflib.Graph()
    G.bind("oda", rdflib.Namespace(oda_ontology_prefix))        

    G.parse(data=combined_ttl, 
            format="turtle")
    logger.info("interpreted turtle: %s", G.serialize(format="turtle"))

    limits_inference(G, param_uri)    
   
    owl_types = list(G.objects(param_uri, a))
    predicate_objects = list(G.predicate_objects(param_uri))
    
    logger.info("types: %s", owl_types)

    for p, o in predicate_objects:
        if p != a:
            logger.info("extra predicate %s: %s", p, o)
    
    if deduce_type:
        logger.info("will deduce type")
        if len(owl_types) == 1 and len(predicate_objects) == 1:
            owl_type = owl_types[0]

            logger.info("have exactly one type predicate, returning %s", owl_type)

            G.remove((param_uri, a, owl_type))
        
        elif len(predicate_objects) > 1:
            owl_type = construct_common_root_class(G, param_uri, predicate_objects)
            logger.info("constructed common type predicate, returning %s", owl_type)

        else:
            raise NotImplementedError("no semantic annotation found")
    else:
        logger.info("will NOT deduce type")
        owl_type = param_uri

    logger.info("complete extra ttl: %s", G.serialize(format="turtle"))
    
    return dict(
        owl_type = str(owl_type),
        extra_ttl = G.serialize(format="turtle"),
    )


def limits_inference(G, root):
    # this works in a peculiar way with OWA since other limits might exist    
    limits = list(G.objects(root, oda['limits']))
    if len(limits) >= 2:
        logger.info("limits: %s", limits)        

        G.add((root, oda['lower_limit'], min(limits)))
        G.add((root, oda['upper_limit'], max(limits)))

        for limit in G.objects(root, oda['limits']):
            G.remove((root, oda['limits'], limit))        
    

def construct_common_root_class(G, param_uri, predicate_objects):
    factors = []

    for p, o in predicate_objects:
        for t in [p, o]:
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
    merged_type = oda_ontology_prefix + "_".join(sorted(factors))
    
    logger.info("merged type %s", merged_type)
    owl_type = merged_type

    for p, o in sorted(predicate_objects):
        G.remove((param_uri, p, o))
        G.add((rdflib.URIRef(merged_type), p, o))

    return owl_type