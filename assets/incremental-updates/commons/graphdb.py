from commons.commons import *
from SPARQLWrapper import SPARQLWrapper, JSON, BASIC, POST, N3
import requests
import time

#GraphDB Utils

def getMetaphactoryConnection():
    sparql = SPARQLWrapper(GRAPHDB_SPARQL)
    sparql.setMethod(POST)
    sparql.setCredentials(GRAPHDB_USER, GRAPHDB_PASSWORD)
    return sparql

def update(q):
    queryConn = getMetaphactoryConnection()
    queryConn.setQuery(q)
    queryConn.query()


def update_dcat_last_modified(list_updates):
    values = " ".join(f""" ( <{update['iri']}> "{update['last_update']}"^^xsd:dateTime ) """ for update in list_updates)
    q =f"""
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        delete {{
            graph ?g {{
                ?dataset dct:modified ?oldTimestamp .
            }}
        }} 
        insert{{
            graph ?g {{
                ?dataset dct:modified ?now .
            }}
        }}
        where {{
            values (?dataset ?now){{
                {values}
            }}.
            graph ?g {{
                ?dataset a <http://www.w3.org/ns/dcat#Dataset>.
                optional {{
                    ?dataset dct:modified ?oldTimestamp .
                }}
            }}
        }}

    """
    update(q)
    


#SPARQL Utils
def build_delete_query(iris, namedGraph):
    values = '\n'.join([f"<{iri}>" for iri in iris if len(iri) > 0])
    q = f"""
        delete {{
            graph ?g {{
                ?s ?p ?o .
                #?o ?pp ?oo .
            }}
        }} where {{
        
            {
               'bind (<'+namedGraph+'> as ?g) . ' if namedGraph else ''
            }    
        
            graph ?g {{
                values ?s {{
                    {values}
                }} .
                ?s ?p ?o .
                #optional {{
                #    ?o ?pp ?oo .
                #    filter not exists {{
                #        ?other ?otherp ?o .
                #        filter (?other != ?s) .
                #    }}
                #}}
            }}
        }}
    """
    return q

def import_and_wait(file):
    data = {
        "importSettings": {
            "name": file,
            "status": "DONE",
            "message": "Imported successfully in less than a second.",
            "context": "",
            "replaceGraphs": [],
            "baseURI": "",
            "forceSerial": False,
            "type": "file",
            "format": None,
            "data": None,
            "timestamp": 1693424058144,
            "parserSettings": {
                "preserveBNodeIds": False,
                "failOnUnknownDataTypes": False,
                "verifyDataTypeValues": False,
                "normalizeDataTypeValues": False,
                "failOnUnknownLanguageTags": False,
                "verifyLanguageTags": False,
                "normalizeLanguageTags": False,
                "stopOnError": False
            }
        },
        "fileNames": [
            file
        ]
    }
    logger.debug(f"Start import of file {file}")
    token = requests.post(GRAPHDB_LOGIN, json={"username":GRAPHDB_USER,"password":GRAPHDB_PASSWORD}).headers['authorization']
    requests.post(GRAPHDB_RUN_IMPORT, json=data, headers={'Authorization':token})
    import_status = "IMPORTING"
    while import_status == "IMPORTING" or import_status == "PENDING":
        time.sleep(10)
        logger.debug(f"Import of file {file} running.")
        response = requests.get(GRAPHDB_CHECK_IMPORT, headers={'Authorization':token}).json()
        import_status = [ current_status for current_status in response if current_status['name']==file ][0] ['status']
    logger.debug(f"Import of file {file} finished with status {import_status}.")

    