import sys
from typing import List, Dict
from SPARQLWrapper import SPARQLWrapper, JSON
from pymongo import MongoClient
from urllib.parse import unquote, urlparse
import yaml
import requests

with open('conf.yml', 'r') as configfile:
    config = yaml.safe_load(configfile)

query = """
SELECT ?item ?longitude ?latitude ?itemLabel ?itemDescription ?urlLabel 
WHERE 
{ 
  ?item wdt:P31/wdt:P279* wd:Q412770; 
        wdt:P18 ?url;
        wdt:P625 ?coordinates.
  BIND(geof:latitude(?coordinates) AS ?latitude)
  BIND(geof:longitude(?coordinates) AS ?longitude)
  
  SERVICE wikibase:label {bd:serviceParam wikibase:language "en".} 
} 
"""

# MediaWiki API endpoint for Wikimedia Commons
COMMONS_API_ENDPOINT = "https://commons.wikimedia.org/w/api.php"

def get_image_dimensions(filename):
    """Fetch image dimensions using the MediaWiki API."""

    parsed_url = urlparse(filename)
    filename = unquote(parsed_url.path.split('/')[-1])
    params = {
        "action": "query",
        "titles": f"File:{filename}",
        "prop": "imageinfo",
        "iiprop": "size",
        "format": "json"
    }
    response = requests.get(COMMONS_API_ENDPOINT, params=params)
    data = response.json()
    pages = data.get("query", {}).get("pages", {})
    for page in pages.values():
        imageinfo = page.get("imageinfo", [{}])[0]
        return {
            "width": imageinfo.get("width"),
            "height": imageinfo.get("height")
        }

def get_collection():
    # Create a connection using MongoClient.
    client = MongoClient(config['mongo']['connection'])
    collection_name = config['wikidata']['collection']
    collection = client[config['mongo']['raw']][collection_name]
    collection.create_index('url', unique=True)
    return collection

class WikiDataQueryResults:
    """
    A class that can be used to query data from Wikidata using SPARQL and return the results as a Pandas DataFrame or a list
    of values for a specific key.
    """
    def __init__(self, query: str, page, size=200):
        """
        Initializes the WikiDataQueryResults object with a SPARQL query string.
        :param query: A SPARQL query string.
        """
        self.user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
        self.endpoint_url = "https://query.wikidata.org/sparql"
        self.sparql = SPARQLWrapper(self.endpoint_url, agent=self.user_agent)
        offset = f"limit {size} OFFSET {size*page}" if page else f"limit {size}"
        self.sparql.setQuery(query+offset)
        self.sparql.setReturnFormat(JSON)

    def __transform2dicts(self, results: List[Dict]) -> List[Dict]:
        """
        Helper function to transform SPARQL query results into a list of dictionaries.
        :param results: A list of query results returned by SPARQLWrapper.
        :return: A list of dictionaries, where each dictionary represents a result row and has keys corresponding to the
        variables in the SPARQL SELECT clause.
        """
        new_results = []
        test = set()
        for result in results:
            new_result = {}
            if result['urlLabel']['value'] in test:
                continue
            for key in result:
                if key == 'itemLabel':
                    new_result['title'] = result[key]['value']
                elif key == 'itemDescription':
                    new_result['description'] = result[key]['value']
                elif key == 'urlLabel':
                    new_result['url'] = result[key]['value']
                else:
                    new_result[key] = result[key]['value']
            test.add(result['urlLabel']['value'])
            new_results.append(new_result)
        return new_results


    def load(self) -> List[Dict]:
        """
        Helper function that loads the data from Wikidata using the SPARQLWrapper library, and transforms the results into
        a list of dictionaries.
        :return: A list of dictionaries, where each dictionary represents a result row and has keys corresponding to the
        variables in the SPARQL SELECT clause.
        """
        results = self.sparql.queryAndConvert()['results']['bindings']
        results = self.__transform2dicts(results)
        return results


if __name__ == '__main__':
    collection = get_collection()
    page = 0
    while True:
        print(page)
        data = WikiDataQueryResults(query, page).load()
        if data:
            for image in data:
                dimensions = get_image_dimensions(image['url'])
                image.update({
                    "width": dimensions.get("width"),
                    "height": dimensions.get("height")
                })
                try:
                    collection.insert_one(image)
                except Exception as e:
                    continue
            #collection.insert_many(data, ordered=False)
        else:
            break
        page += 1