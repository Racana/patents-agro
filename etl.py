import pandas as pd
import json, os, sys, gzip
from google.cloud import storage
from tqdm import tqdm
from datetime import datetime

client = storage.Client()
BUCKET_NAME = 'patents-agro' 


def list_blobs(client, bucket_name):
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='test/us_publications_')
    blobs = list(blobs)

    return blobs

def download_blobs(blobs, download_folder):
    filenames = []
    print(f"Downloading {len(blobs)} blobs from Google Console to {download_folder}")
    for blob in tqdm(blobs):        
        filename = blob.name.split("/")[-1]
        full_path = os.path.join(download_folder, filename)

        if not os.path.exists(full_path):
            blob.download_to_filename(full_path)

        filenames.append(full_path)

    return filenames

def read_json(filename):
    gz =  gzip.open(filename)
    file = gz.read()
    json_data = [json.loads(row.decode('utf-8')) for row in file.split(b'\n') if row]

    return json_data

def extract_patent_text(json_data, patent_folder):
    """Extract patent text data from jsonfile"""
    patent = json_data[0]
    for patent in json_data:
        publication_number = patent['publication_number']
        try:
            title = patent['title_localized'][0]['text']
        except IndexError:
            title = ""
        try:
            abstract = patent['abstract_localized'][0]['text']
        except IndexError:
            abstract = ""
        try:
            claims = patent['claims_localized'][0]['text']
        except IndexError:
            claims = ""
        try:
            description = patent['description_localized'][0]['text']
        except IndexError:
            description = ""

        patent_text = "\n".join([title, abstract, claims, description])

        save_patent_text(publication_number, patent_text, patent_folder)
            
    return None

def save_patent_text(publication_number, text, patent_folder):
    text_filename = os.path.join(patent_folder, publication_number + ".txt")

    text_file = open(text_filename, "wt", encoding="utf-8")
    n = text_file.write(text)
    text_file.close()

    return None

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

        publication_date = patent['publication_date']
        filing_date = patent['filing_date']
        grant_date = patent['grant_date']
        
        try:
            title = patent['title_localized'][0]['text']
        except IndexError:
            title = None

        for inventor in patent['inventor_harmonized']:
            inventor_list.append(inventor['name'])
            inventor_list_cc.append(inventor['country_code'])

        for assignee in patent['assignee_harmonized']:
            assignee_list.append(assignee['name'])
            assignee_list_cc.append(assignee['country_code'])

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
                                assignee_list, assignee_list_cc, amount_citation, citations, 
                                parents, parents_dates, childs, childs_dates))

    return patents_metadata

def metadata_dataframe(patents_metadata):
    colnames_metadata = ['publication_number', 'country_code', 'title', 'publication_date', 
                'filing_date', 'grant_date', 'inventor_list', 'inventor_list_cc', 
                'assignee_list', 'assignee_list_cc', 'amount_citation', 'citations', 
                'parents', 'parents_dates', 'childs', 'childs_dates']
    metadata = pd.DataFrame(data=patents_metadata, columns=colnames_metadata)

    return metadata

def etl(download_folder, patent_folder):
    patents_metadata = []
    blobs = list_blobs(client, BUCKET_NAME)
    filenames = download_blobs(blobs, download_folder)
    for json_file in filenames:
        json_data = read_json(json_file)
        extract_patent_text(json_data, patent_folder)
        patents_metadata = extract_patent_metadata(json_data, patents_metadata)
    
    metadata = metadata_dataframe(patents_metadata)
    metadata.to_pickle(os.path.join(folder, "metadata.pkl"))

    return None


if __name__ == '__main__':

    start = datetime.now() 
    script, folder = sys.argv
    download_folder = "download"
    patent_folder = "patent"
    download_folder = os.path.join(folder, download_folder)
    patent_folder = os.path.join(folder, patent_folder)

    if not os.path.exists(download_folder):
        os.mkdir(download_folder)
    
    if not os.path.exists(patent_folder):
        os.mkdir(patent_folder)

    etl(download_folder, patent_folder)

    print(datetime.now()-start)