import json
from google.cloud import storage

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
    patents_json = [json.loads(row.decode('utf-8')) for row in json_data.split(b'\n') if row]

    return patents_json


def extract_patent_data(patents_json, patents):
    """Downloads and extract patent data from GCS"""
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

        patents.append((publication_number, title, abstract, claims, description, 
                    publication_date, filing_date))
        
    return patents

def run():
    patents = []
    blobs = list_all_files(client, BUCKET_NAME)
    for blob in blobs:
        patents_json = download_blob(blob)
        patents = extract_patent_data(patents_json, patents)

    colnames = ['publication_number', 'title', 'abstract', 'claims', 'description', 
            'publication_date', 'filing_date']
    df = pd.DataFrame(data=patents, columns=colnames)

    return df.to_csv('patents_text.csv')