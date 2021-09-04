import pandas as pd
import json
from google.cloud import storage
from tqdm import tqdm

client = storage.Client()

BUCKET_NAME = 'patents-agro'

def list_all_files(client, bucket_name):
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='patents/patents_publications_us_')
    blobs = list(blobs)

    return blobs

def download_blob(blob):
    """Downloads a blob from the bucket."""

    json_file = blob.download_as_string()
    patents_json = [json.loads(row.decode('utf-8')) for row in json_file.split(b'\n') if row]

    return patents_json

def extract_patent_metadata(patents_json, patents_metadata):
    for patent in patents_json:
        inventor_list = []
        inventor_list_cc = []
        assignee_list = []
        assignee_list_cc = []
        citations = []
        parents = []
        parents_dates = []
        childs = []
        childs_dates = []

        publication_number = patent['publication_number']
        country_code = patent['country_code']
        try:
            title = patent['title_localized'][0]['text']
        except IndexError:
            title = None
        publication_date = patent['publication_date']
        filing_date = patent['filing_date']
        grant_date = patent['grant_date']
        
        for inventor in patent['inventor_harmonized']:
            inventor_list.append(inventor['name'])
            inventor_list_cc.append(inventor['country_code'])

            for assignee in patent['assignee_harmonized']:
                assignee_list.append(assignee['name'])
                assignee_list_cc.append(assignee['country_code'])

            code = patent['code']
            inventive = patent['inventive']
            first = patent['first']
            amount_citation = len(patent['citation'])

                for citation in patent['citation']:
                    citations.append(citation['publication_number'])

                    for parent in patent['parent']:
                        parents.append(parent['application_number'])
                        parents_dates.append(parent['filing_date'])
                    
                        for child in patent['child']:
                            childs.append(child['application_number'])
                            childs_dates.append(child['filing_date'])

                        patents_metadata.append((publication_number, country_code, title, publication_date, 
                                                filing_date, grant_date, inventor_list, inventor_list_cc, 
                                                assignee_list, assignee_list_cc, code, inventive, first, 
                                                amount_citation, citations, parents, parents_dates, childs, 
                                                childs_dates))


    return patents_metadata



def extract_patent_text(patents_json, patents_text):
    """Extract patent text data from jsonfile"""
    for patent in patents_json:
        publication_number = patent['publication_number']
        try:
            title = patent['title_localized'][0]['text']
        except IndexError:
            title = None
        try:
            abstract = patent['abstract_localized'][0]['text']
        except IndexError:
            abstract = None
        try:
            claims = patent['claims_localized'][0]['text']
        except IndexError:
            claims = None
        try:
            description = patent['description_localized'][0]['text']
        except IndexError:
            description = None

        publication_date = patent['publication_date']
        filing_date = patent['filing_date']

        patents_text.append((publication_number, title, abstract, claims, 
                    publication_date, filing_date))
        
    return patents_text


patents_text = []
patents_metadata = []
blobs = list_all_files(client, BUCKET_NAME)
for blob in tqdm(blobs):
    patents_json = download_blob(blob)
    patents_text = extract_patent_text(patents_json, patents_text)
    patents_metadata = extract_patent_metadata(patents_json, patents_metadata)

colnames_text = ['publication_number', 'title', 'abstract', 'claims', 
                'publication_date', 'filing_date']
text = pd.DataFrame(data=patents_text, columns=colnames_text)
text.to_csv('patents_text.csv')

colnames_metadata = ['publication_number', 'country_code', 'title', 'publication_date', 
                'filing_date', 'grant_date', 'inventor_list', 'inventor_list_cc', 
                'assignee_list', 'assignee_list_cc', 'code', 'inventive', 'first', 
                'amount_citation', 'citations', 'parents', 'parents_dates', 'childs', 
                'childs_dates']
metadata = pd.DataFrame(data=patents_metadata, columns=colnames_metadata)
metadata.to_pickle(f"metadata.pkl")