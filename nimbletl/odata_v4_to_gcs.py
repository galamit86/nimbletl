import os
from pathlib import Path
import requests
import json
# import pyarrow as pa
from pyarrow import json as pa_json
import pyarrow.parquet as pq
from google.cloud import storage


def create_dir(path: Path) -> Path:
    """Checks whether path exists and is directory, and creates it if not.
    
    Args:
        - path (Path): path to check
    
    Returns:
        - Path: new directory
    """
    try:
        path = Path(path)
        if not (path.exists() and path.is_dir()):
            path.mkdir(parents=True)
        return path
    except TypeError as error:
        print(f"Error trying to find {path}: {error!s}")
        return None


def get_table_description_v4(url_table_properties):
    """Gets table description of a table in CBS odata V4.

    Args:
        - url_table_properties (str): url of the data set `Properties`
    
    Returns:
        - String: table_description
    """
    r = requests.get(url_table_properties).json()
    return r['Description']


def get_odata_v4(target_url):
    """Gets a table from a specific url for CBS Odata v4.

    Args:
        - url_table_properties (str): url of the table
    
    Returns:
        - data (list): all data received from target url as json type, appeneded in one list
    """
    data = []
    while target_url:
        r = requests.get(target_url).json()
        data.extend(r['value'])
        
        if '@odata.nextLink' in r:
            target_url = r['@odata.nextLink']
        else:
            target_url = None
            
    return data


def cbsodatav4_to_gcs(id, schema='cbs', third_party=False):
    base_url = {
        True: None,  # currently no IV3 links in ODATA V4,
        False: f"https://odata4.cbs.nl/CBS/{id}"
    }
    urls = {
        item['name']: base_url[third_party] + "/" + item['url']
        for item in get_odata_v4(base_url[third_party])
    }
    # Get the description of the data set
    data_set_description = get_table_description_v4(urls["Properties"])

    # gcs_bucket = gcs.bucket(GCP.bucket)

    # Create placeholders
    files_parquet = set()
    create_dir("./temp/ndjson")
    create_dir("./temp/parquet")

    ## Downloading datasets from CBS and converting to Parquet

    # Iterate over all tables related to dataset, excepet Properties (TODO -> double check that it is redundandt)
    for key, url in [
        (k, v) for k, v in urls.items() if k not in ("Properties")
    ]:

        # Create table name to be used in GCS
        table_name = f"{schema}.{id}_{key}"
        # File to dump table as ndjson
        ndjson_path = f"./temp/ndjson/{table_name}.ndjson"
        # File to create as parquet file
        pq_path = f"./temp/parquet/{table_name}.parquet"

        # get data from source
        table = get_odata_v4(url)

        # Dump as ndjson format (TODO -> IS THERE A FASTER/BETTER WAY??)
        with open(ndjson_path, 'w+') as ndjson:
            for record in table:
                ndjson.write(json.dumps(record) + "\n")

        # Create PyArrow table from ndjson file
        pa_table = pa_json.read_json(ndjson_path)

        # Store parquet table
        pq.write_table(pa_table, pq_path)

        # Add path of file to set
        files_parquet.add(pq_path)

    ## Uploading to GCS

    # Initialize Google Storage Client, get bucket, set blob (TODO -> consider structure in GCS)
    gcs = storage.Client(project="dataverbinders-dev")
    gcs_bucket = gcs.get_bucket("dataverbinders-dev_test")
    # gcs = storage.Client(project=GCP.project)  #when used with GCP Class
    for pfile in os.listdir('./temp/parquet/'):
        gcs_blob = gcs_bucket.blob(pfile)
        gcs_blob.upload_from_filename("./temp/parquet/"+pfile)

    return files_parquet, data_set_description

cbsodatav4_to_gcs("82807NED")




    # # Upload to GCS
    # # Name of file in GCS.
    # gcs_blob = gcs_bucket.blob(pq_dir.split("/")[-1])

    # # Upload file to GCS from given location.
    # gcs_blob.upload_from_filename(filename=pq_dir)

    # # Name of file in GCS.
    # gcs_blob = gcs_bucket.blob(pq_dir.split("/")[-1])

    # # Upload file to GCS from given location.
    # gcs_blob.upload_from_filename(filename=pq_dir)
                
    # return files_parquet, data_set_description