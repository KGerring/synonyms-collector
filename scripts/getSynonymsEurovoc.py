from rdflib import Graph
from SPARQLWrapper import TURTLE, SPARQLWrapper

sparql = SPARQLWrapper("http://localhost:7200/repositories/eurovoc")
sparql.setQuery("""
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
  PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
  CONSTRUCT {
      ?s rdf:type skos:Concept.
      ?s skos:prefLabel ?prefLabel .
      ?s skos:altLabel ?alt .
  }
  where { 
    ?s skos:inScheme ?o .
      ?o skos:prefLabel ?Scheme .
      FILTER (lang(?Scheme) = 'en') .
      ?s rdf:type skos:Concept.
      ?s skos:prefLabel ?prefLabel .
      ?s skosxl:altLabel ?altLabel .
      ?altLabel skosxl:literalForm  ?alt .
      FILTER (lang(?prefLabel) = 'en') .
      FILTER (lang(?alt) = 'en') .
  }
""")

sparql.setReturnFormat(TURTLE)
results = sparql.query().convert()

g = Graph()
g.parse(data=results, format="turtle")
g.serialize(destination='syn_eurovoc2.ttl', format='turtle')
