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
from SPARQLWrapper import JSON, SPARQLWrapper
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

def synonymsFromSPARQLEndpoint(endpoint, term):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?found
        WHERE
            {
                {
                    SELECT ?found
                    WHERE {
                        ?term rdf:type skos:Concept .
                        ?term skos:prefLabel ?termlabel .
                        ?term skos:altLabel ?altlabel .
                        FILTER (lcase(?termlabel) = '""" + term.lower() + """'@en)
                        FILTER(lang(?altlabel)="en" || lang(?altlabel)="")
                        BIND (?altlabel as ?found)
                    }
                }
                UNION
                {
                    SELECT ?found
                    WHERE {
                        ?term rdf:type skos:Concept .
                        ?term skos:prefLabel ?termlabel .
                        ?term skos:altLabel ?altlabel .
                        FILTER (lcase(?altlabel) = '""" + term.lower() + """'@en)
                        FILTER(lang(?termlabel)="en" || lang(?termlabel)="")
                        BIND (?termlabel as ?found)
                    }
                }
            }
        """)

    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    resultslist = []
    for result in results["results"]["bindings"]:
        label = result["found"]["value"]
        resultslist.append(label)

    return resultslist

def synonymsFromSPARQLEndpointLOV(endpoint, term):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT ?found
        WHERE
            {
                {
                    SELECT ?found
                    WHERE {
                        ?term skos:prefLabel ?termlabel .
                        ?term skos:altLabel ?altlabel .
                        FILTER (lcase(?termlabel) = '""" + term.lower() + """'@en || lcase(?termlabel) = '""" + term.lower() + """')
                        FILTER(lang(?altlabel)="en" || lang(?altlabel)="")
                        BIND (?altlabel as ?found)
                    }
                }
                UNION
                {
                    SELECT ?found
                    WHERE {
                        ?term skos:prefLabel ?termlabel .
                        ?term skos:altLabel ?altlabel .
                        FILTER (lcase(?altlabel) = '""" + term.lower() + """'@en || lcase(?altlabel) = '""" + term.lower() + """' )
                        FILTER(lang(?termlabel)="en" || lang(?termlabel)="")
                        BIND (?termlabel as ?found)
                    }
                }
                UNION
                {
                    SELECT ?found
                    WHERE {
                        ?term rdfs:label ?termlabel .
                        ?term skos:altLabel ?altlabel .
                        FILTER (lcase(?termlabel) = '""" + term.lower() + """'@en || lcase(?termlabel) = '""" + term.lower() + """')
                        FILTER(lang(?altlabel)="en" || lang(?altlabel)="")
                        BIND (?altlabel as ?found)
                    }
                }
                UNION
                {
                    SELECT ?found
                    WHERE {
                        ?term rdfs:label ?termlabel .
                        ?term skos:altLabel ?altlabel .
                        FILTER (lcase(?altlabel) = '""" + term.lower() + """'@en || lcase(?altlabel) = '""" + term.lower() + """' )
                        FILTER(lang(?termlabel)="en" || lang(?termlabel)="")
                        BIND (?termlabel as ?found)
                    }
                }
            }
        """)

    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    resultslist = []
    for result in results["results"]["bindings"]:
        label = result["found"]["value"]
        resultslist.append(label)

    return resultslist
def synonymsDatamuse(term):
    api = datamuse.Datamuse()
    response = api.words(ml=term.lower())
    resultslist = []
    counter = 0
    for i in response:
        if(i.get('tags') and ('syn' in i['tags'])):
            word = i['word'].lower().replace("_", " ")
            resultslist.append(word)
    return resultslist

def synonymsAltervista(term, endpoint, apikey):
    word = '?word=' + term
    language = '&language=' + 'en_US'
    key = '&key=' + apikey
    output = '&output=' + 'json'
    url = endpoint + word + language + key + output
    arrayWithoutAntonyms = []
    try:
        response = requests.get(url).json()
        # print(response)
        syns = ''
        if 'error' not in response:
            for i in response['response']:
                # if i['list']['category'] == '(noun)':
                syns = syns + i['list']['synonyms'] + '|'
            arraylist = syns.split("|")
            arraylist = [a for a in arraylist if a]
            # arraylist = [re.sub(r' \(generic term\)', r'', a) for a in arraylist]
            arraylist = [re.sub(r' \(related term\)', r'', a) for a in arraylist]
            arraylist = [re.sub(r' \(similar term\)', r'', a) for a in arraylist]
            arraylist = [a.replace("_", " ") for a in arraylist]
            lower_case_list = []
            for a in arraylist:
                if not a.isupper():
                    a = a.lower()
                lower_case_list.append(a)
            arrayWithoutGenericTerms = [x for x in lower_case_list if 'generic term' not in x]
            arrayWithoutAntonyms = [x for x in arrayWithoutGenericTerms if 'antonym' not in x]
    except requests.exceptions.RequestException as e:
        print(e)
    return arrayWithoutAntonyms

def get_config(file):
    my_path = Path(__file__).resolve()  # resolve to get rid of any symlinks
    config_path = my_path.parent / file
    with config_path.open() as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config

config = get_config("configSynonyms.yaml")
ALTERVISTA_KEY = config['input']['api']['altervista']['key']
INPUT_FILE = config['input']['file']['name']
OUTPUT_FILE = config['output']['file']['name']
PARSER = argparse.ArgumentParser(description="Enrich skos list with synonyms")
PARSER.add_argument("-k", "--apikey", help="api key for Altervista")
PARSER.add_argument("-i", "--input", help="input file in RDF/XML")
PARSER.add_argument("-o", "--output", help="output file in Turtle")
ARGS = PARSER.parse_args()
if ARGS.apikey:
    ALTERVISTA_KEY = ARGS.apikey
if ARGS.input:
    INPUT_FILE = ARGS.input
if ARGS.output:
    OUTPUT_FILE = ARGS.output

SKOSXL = Namespace("http://www.w3.org/2008/05/skos-xl#")
g = Graph()
g.parse(INPUT_FILE , format=config['input']['file']['format'])

triple = g.triples((None, RDFS.label, None))
length = len(list(triple))

with tqdm(total=length) as pbar:
    for a, b, c in g.triples((None, RDFS.label, None)):
        pbar.update(1)
        total_syns = 0

        c = split_merge(c)

        mylist = []
        mylist = list(set(mylist))

        pbar.set_description("Searching %s in Datamuse..." % c)
        syns1 = synonymsDatamuse(c)
        if(syns1):
            for z in syns1:
                mylist.append([z,"Datamuse"])

        pbar.set_description("Searching %s in Altervista..." % c)
        syns9 = synonymsAltervista(c, config['input']['api']['altervista']['endpoint'], ALTERVISTA_KEY)
        if(syns9):
            for label in syns9:
                mylist.append([str(label),"Altervista"])

        repository_list = config['input']['repository']
        '''
        for repository in repository_list:
            pbar.set_description("Searching " + c + " in " + repository['name'])
            if (repository['name'] == "Lov"):
                syns = synonymsFromSPARQLEndpointLOV(repository['endpoint'], c)
            else:
                syns = synonymsFromSPARQLEndpoint(repository['endpoint'], c)
            if(syns):
                for label in syns:
                    # wiktionary and wordnet use _ to separate words
                    label = label.replace("_"," ")
                    mylist.append([str(label),repository['name']])
        '''
        if len(mylist) == 0:
            mylist.append([c,"origin"])   
        for element in mylist:
            labelURI = element[0].replace("_","-").replace(" ","-").replace("(","-").replace(")","-").replace(",","-").replace("*","-").replace("&amp;","-").replace(".","-").replace("'","-")
            if(config['alternative'] == "skos"):
                g.add((a, SKOS.altLabel, Literal(element[0], lang="en")))
            else:
                altLabelURI = URIRef(config['skosxl']['baseuri'] + labelURI)
                g.add((a, SKOSXL.altLabel, altLabelURI))
                g.add((altLabelURI, SKOSXL.literalForm, Literal(element[0], lang="en")))
                g.add((altLabelURI, DC.source, Literal(element[1], lang="en")))
                g.add((altLabelURI, RDF.type, SKOSXL.Label))

g.serialize(destination=OUTPUT_FILE, format=config['output']['file']['format'])
