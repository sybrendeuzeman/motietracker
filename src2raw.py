import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
import sqlite3
import time

conn = sqlite3.connect('data.db')
cursor = conn.cursor()

query_create_table = """CREATE TABLE IF NOT EXISTS referenties (
    identifierDocument text NOT NULL, 
    identifierReferentie text NOT NULL);"""
cursor.execute(query_create_table)
conn.commit

query_insert_into = """
    INSERT INTO referenties(identifierDocument, identifierReferentie)
    VALUES (?, ?)
    """

def getRefs(document):
    identifier_OB_document = document['DocumentVersie'][0]['ExterneIdentifier']
    url_OB_xml = f'https://zoek.officielebekendmakingen.nl/{identifier_OB_document}.xml'
    
    responseXML = requests.get(url_OB_xml)
    docSoup = BeautifulSoup(responseXML.text)

    refsDocumenten = docSoup.find_all('extref', soort = 'document')

    [cursor.execute(query_insert_into, [identifier_OB_document, ref.get('doc')]) for ref in refsDocumenten]
    time.sleep(1)


url = 'https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0/document?$expand=documentversie($filter=ExterneIdentifier%20ne%20null)&$filter=documentversie/any(d:%20d/ExterneIdentifier%20ne%20null)&$count=true'

response = requests.get(url)
if response.status_code == 200:
    json_data = json.loads(response.text)

[getRefs(document) for document in json_data['value']]

conn.commit()
