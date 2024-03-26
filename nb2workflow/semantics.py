import re
import uuid
import logging
import rdflib

logger = logging.getLogger(__name__)

oda_ontology_prefix = "http://odahub.io/ontology#"

oda = rdflib.Namespace(oda_ontology_prefix)
unit = rdflib.Namespace(oda_ontology_prefix.rstrip("#") + "/unit#")

xsd = rdflib.Namespace("http://www.w3.org/2001/XMLSchema#")
rdfs = rdflib.Namespace("http://www.w3.org/2000/01/rdf-schema#")
rdf = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")                        

rdf_prefixes = {'oda': oda, 
                'xsd': xsd, 
                'rdf': rdf, 
                'rdfs': rdfs, 
                'unit': unit}

a = rdf['type']
subClassOf = rdfs['subClassOf']



# TODO: register this function as versioned rdf-generating workflow
def understand_comment_references(comment, base_uri=None, fallback_type=None) -> dict:
    if base_uri is None:
        base_uri = oda[uuid.uuid1().hex]
        deduce_type = True
    else:
        deduce_type = False            

    comment = comment.strip()    

    logger.debug('understand_comment_references: "%s"', comment)

    # this allows to use simplified syntax in some cases, e.g. when just a url alone is provided to indicate type
    comment = re.sub(rf"\b(http.*?)(?:\s|$)", r"<\1>", comment)
    comment = re.sub(rf"([0-9])\.( |$)", r"\1.0\2", comment)

    logger.debug('preprocessed comment: "%s"', comment)
            
    parsed = None
    parse_failures = []

    variations = [
        f"{comment}",
        f"{base_uri.n3()} a {comment} .",
        f"{base_uri.n3()} {comment} .",
        f"{base_uri.n3()} a {comment}",
        f"{base_uri.n3()} {comment}",
        # "{base_uri.n3()} rdfs:subClassOf {comment} .",
    ]

    if fallback_type is not None:
        variations.append(f"{base_uri.n3()} a <{fallback_type}>; {comment} .")

    prefixes_in_string = "\n".join([f"@prefix {p}: <{n}> ." for p, n in rdf_prefixes.items()])
    
    for variation in variations:
        try:            
            parsed = parse_ttl(prefixes_in_string + "\n"*3 + variation, base_uri, deduce_type)
            logger.info("this variation WAS parsed: %s to %s", variation, parsed)
        except (rdflib.plugins.parsers.notation3.BadSyntax, NotImplementedError, IndexError) as e:
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
    G.bind("oda", oda)
    G.bind("unit", unit)
    
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
    """
    sometimes, an instance has a predicate-object which may be common to many instances. 
       then, there is a presumed new class which includes instances sharing this predicate
          then, the instance has an additional presumed class
    sometimes, an instance has several types, either through presumed class mechanism above or by other means
       then, the instance can be understood to have a type of another, singular, class, which can be constructed
    this process is a mechanism of construction, evolution of classes

    for example, instances of fruit which are round, red, and sweet are apples. apple is a new class, which includes all individual apples.
       "an apple" could be also seen as an instance in the ontology of fruits. 
           but it is not possible to further derive (subclass, specialize, inherit) from instances
           we prefer to consider apple a class, and potentially create new subclasses, e.g. pink lady apple
              this makes the ontology extensible
           individual instances of apples are things as such, and we can only point to them and describe their classes and properties
              we can point to individual parameters used in particular workflows 
                 they are instances, and they derive their characteristics from their classes

    ===

    so we treat parameter types as classes, and parameters as used in the workflows as instances
    to simplify expression in the notebook parameter annotation, we use annotation properties like oda:upper_limit. 
        these are later transformed by the consumer (e.g. oda dispatcher) into owl restrictions

    """

    factors = []

    for p, o in predicate_objects:
        logger.info("p, o pair in in merge type: %s %s", p, o)
        for t in [p, o]:
            t = t.n3()
            for common_factor in [a] + list(rdf_prefixes.values()):
                t = t.replace(common_factor, "")

            t = re.sub("[^a-zA-Z0-9_]", "", t)
            
            if t != "":
                logger.info("factor in merge type: %s", t)
                factors.append(t)
        
    # it does not matter exactly how this is formatted as long as it is unique
    # it is good that it is readable
    merged_type = oda["_".join(sorted(factors))]
    
    logger.info("merged type %s", merged_type)
    owl_type = merged_type

    for p, o in sorted(predicate_objects):
        G.remove((param_uri, p, o))
        if p == a:
            G.add((merged_type, subClassOf, o))
        else:
            G.add((merged_type, p, o))
 

    return owl_type