from rdflib import Graph
from SPARQLWrapper import TURTLE, SPARQLWrapper

sparql = SPARQLWrapper("http://localhost:7200/repositories/fibo")
sparql.setQuery("""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX cmns-av: <https://www.omg.org/spec/Commons/AnnotationVocabulary/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    CONSTRUCT {
      ?term rdf:type skos:Concept .
      ?term skos:prefLabel ?termlabel .
      ?term skos:altLabel ?synlabel .
    }
    WHERE {
      ?term rdfs:label ?label .
      ?term cmns-av:synonym ?synlabel .
      FILTER (lang(?label) = 'en' || lang(?label) = '') .
      FILTER (lang(?synlabel) = 'en' || lang(?synlabel) = '') .
      BIND(if (lang(?label) = '',strlang(?label,"en"),?label) as  ?termlabel) .
    }
""")

sparql.setReturnFormat(TURTLE)
results = sparql.query().convert()

g = Graph()
g.parse(data=results, format="turtle")
g.serialize(destination='syn_fibo.ttl', format='turtle')
