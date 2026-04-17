"""
.. module:: synonyms_harvester
   :platform: Unix, Windows
   :synopsis: A python script to enrich a labels with synonyms taken from different sources
.. moduleauthor:: Emidio Stani <emidio.s.stani@pwc.com>
"""

import argparse
import re
from re import finditer
import requests
from datamuse import datamuse
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, RDFS, DC, SKOS
from SPARQLWrapper import TURTLE, RDFXML, SPARQLWrapper
from tqdm import tqdm
from pathlib import Path
import yaml

def camel_case_split(identifier):
    matches = finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]

def merge_lower_array(my_list):
    return " ".join(item.lower() for item in my_list)

def split_merge(identifier):
    return merge_lower_array(camel_case_split(identifier))

def getDCMapping(endpoint, uri):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        CONSTRUCT {?subject ?relation ?object}
        WHERE {
            ?subject ?relation ?object
            FILTER (str(?subject) = '""" + uri + """' || str(?object) = '""" + uri + """')
        }
    """)

    sparql.setReturnFormat(TURTLE)
    results = sparql.query().convert()
    return results

def getWDMapping(endpoint, label):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        CONSTRUCT {
            ?schema ?relation ?subject .
            ?subject rdfs:label ?label .
        }
        WHERE {
            ?subject rdfs:label ?label .
            FILTER (?label = '""" + label + """') .
            ?schema ?relation ?subject .
            FILTER (strstarts(str(?schema), "https://schema.org/"))
        }
    """)

    sparql.setReturnFormat(TURTLE)
    results = sparql.query().convert()
    return results

def getDBpediaMapping(endpoint, label):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        CONSTRUCT {
            ?subject ?relation ?schema  .
            ?subject rdfs:label ?label .
        }
        WHERE {
            ?subject rdfs:label ?label .
            FILTER (?label = '""" + label + """'@en) .
            ?subject ?relation ?schema  .
            FILTER (strstarts(str(?schema), "http://schema.org/"))
        }
    """)

    sparql.setReturnFormat(TURTLE)
    results = sparql.query().convert()
    return results

def getWikidataMapping(endpoint, label):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        CONSTRUCT {
            ?item ?relation  ?schema .
            ?item rdfs:label ?mylabel .
        }
        WHERE {
            SERVICE wikibase:mwapi {
                bd:serviceParam wikibase:endpoint "www.wikidata.org";
                wikibase:api "EntitySearch";
                mwapi:search '""" + label + """';
                mwapi:language "en".
                ?item wikibase:apiOutputItem mwapi:item.
            }
            ?item rdfs:label ?mylabel
            FILTER(?mylabel = '""" + label + """'@en)
            ?item ?relation ?schema  .
            FILTER (strstarts(str(?schema), "https://schema.org/"))
        }
    """)

    sparql.setReturnFormat(RDFXML)
    results = sparql.query().convert()
    return results

def getLovMapping(endpoint, label):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        CONSTRUCT {
            ?subject ?relation ?schema  .
            ?subject rdfs:label ?label .
        }
        WHERE {
            ?subject rdfs:label ?label .
            FILTER (?label = '""" + label + """'@en) .
            ?subject ?relation ?schema  .
            FILTER (strstarts(str(?schema), "http://schema.org/")  || strstarts(str(?schema), "https://schema.org/"))
        }
    """)

    sparql.setReturnFormat(TURTLE)
    results = sparql.query().convert()
    return results

def get_config(file):
    my_path = Path(__file__).resolve()  # resolve to get rid of any symlinks
    config_path = my_path.parent / file
    with config_path.open() as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config

config = get_config("configIndirectMapping.yaml")
INPUT_FILE = config['input']['file']['name']
OUTPUT_FILE = config['output']['file']['name']
PARSER = argparse.ArgumentParser(description="Enrich skos list with synonyms")
PARSER.add_argument("-i", "--input", help="input file in RDF/XML")
PARSER.add_argument("-o", "--output", help="output file in Turtle")
ARGS = PARSER.parse_args()
if ARGS.input:
    INPUT_FILE = ARGS.input
if ARGS.output:
    OUTPUT_FILE = ARGS.output

inputGraph = Graph()
outputGraph = Graph()
inputGraph.parse(INPUT_FILE , format=config['input']['file']['format'])

triple = inputGraph.triples((None, RDFS.label, None))
length = len(list(triple))

with tqdm(total=length) as pbar:
    for a, b, c in inputGraph.triples((None, RDFS.label, None)):
        pbar.update(1)
        total_syns = 0

        label = split_merge(c)
        
        mylist = []
        mylist = list(set(mylist))

        pbar.set_description("Searching %s in Dublin Core Mapping..." % label)
        endpoint = config['input']['mapping']['dcschema']
        # print(endpoint)
        dcMapping =  getDCMapping(endpoint, str(a))
        dcGraph = Graph()
        dcGraph.parse(data=dcMapping, format="turtle")
       
        for sub, obj, pred in dcGraph:
            # print("Found " + str(pred))
            outputGraph.add((sub,obj,pred))

        pbar.set_description("Searching %s in Wikidata Mapping..." % label)
        endpoint = config['input']['mapping']['wdschema']
        # print(endpoint)
        wdMapping =  getWDMapping(endpoint, label)
        wdGraph = Graph()
        wdGraph.parse(data=wdMapping, format="turtle")
       
        for sub, obj, pred in wdGraph:
            outputGraph.add((sub,obj,pred))

        pbar.set_description("Searching %s in DBpedia..." % label)
        endpoint = config['input']['mapping']['dbpedia']
        # print(endpoint)
        dbpediaMapping =  getDBpediaMapping(endpoint, label)
        dbpediaGraph = Graph()
        dbpediaGraph.parse(data=dbpediaMapping, format="turtle")
       
        for sub, obj, pred in dbpediaGraph:
            print("Found: " + str(pred))
            outputGraph.add((sub,obj,pred))

        pbar.set_description("Searching %s in Wikidata..." % label)
        endpoint = config['input']['mapping']['wikidata']
        # print(endpoint)
        wikidataGraph =  getWikidataMapping(endpoint, label)
       
        for sub, obj, pred in wikidataGraph:
            # print("Found: " + str(pred))
            outputGraph.add((sub,obj,pred))

        """
        pbar.set_description("Searching %s in LOV..." % label)
        endpoint = config['input']['mapping']['lov']
        # print(endpoint)
        lovMapping =  getLovMapping(endpoint, label)
        lovGraph = Graph()
        lovGraph.parse(data=lovMapping, format="turtle")
       
        for sub, obj, pred in lovGraph:
            print("Found: " + str(pred))
            outputGraph.add((sub,obj,pred))
        """

outputGraph.serialize(destination=OUTPUT_FILE, format=config['output']['file']['format'])
