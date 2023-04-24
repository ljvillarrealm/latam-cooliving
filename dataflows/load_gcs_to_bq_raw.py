### Start
# Libraries
import re
import datetime
from prefect import flow, task
from prefect_gcp import GcpCredentials

from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_cloud_storage
from prefect_gcp.bigquery import BigQueryWarehouse
from prefect_gcp.bigquery import bigquery_load_file

# Terminal color messages
def prGreen(prt):
    """Print a green terminal message"""
    print(f"\033[92m{prt}\033[00m")
def prYellow(prt):
    """Print a yellow terminal message"""
    print(f"\033[93m{prt}\033[00m")
def prRed(prt):
    """Print a red terminal message"""
    print(f"\033[91m{prt}\033[00m")
def prCyan(prt):
    """Print a cyan terminal message"""
    print(f"\033[96m{prt}\033[00m")


### Tasks
#@task(name="load_gcs_to_bg.py : get list of files in the bucket", retries=3)
def get_project_name(gcp_credentials_block_name: str) -> str:
    load_gcp_credentials_block = GcpCredentials.load(gcp_credentials_block_name)
    project_name = load_gcp_credentials_block.__getattribute__("project")

    return project_name

#@task(name="load_gcs_to_bg.py : get list of files in the bucket", retries=3)
def get_list_of_blobs(bucket_credential: str) -> list[ str, list[str]]:
    gcs_bucket = GcsBucket.load(bucket_credential)
    bucket_name = gcs_bucket.__getattribute__("bucket")
    raw_bob_list = gcs_bucket.list_blobs()

    bob_list = []
    for raw_bob in raw_bob_list:
        bob_list.append(re.findall(r'raw\/.*\.parquet', str(raw_bob))[0])

    return [bucket_name, bob_list]

#@task(name="load_gcs_to_bg.py : load cloud storage to bigquery", retries=3)
def load_cloud_storage_to_bigquery(gcs_bigquerywarehouse_block_name: str, project_name: str, dataset_name: str, bucket_name: str, relative_file_path: str) -> None:
    #gcp_credentials = GcpCredentials(project="project")

    project = project_name
    dataset = dataset_name
    table = re.findall(r"raw\/(.*?)\.parquet", relative_file_path)[0]
    table = f'{table}'
    file_bucket_path = f'{bucket_name}/{relative_file_path}'
    url = f'https://storage.cloud.google.com/{file_bucket_path}'
    format = f'PARQUET'

    with BigQueryWarehouse.load(gcs_bigquerywarehouse_block_name) as warehouse:
        operation = f'''
            CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.{table}`
            OPTIONS (
                format = '{format}'
                , uris = ['{url}']
            );
        '''
        result = warehouse.execute(operation)

        if (result is None):
            result = f'OK' # None is OK when excecuting the warehouse operation.

    return result


### Flows
@flow(name="load_gcs_to_bg.py : Main", log_prints=True, retries=3)
def main() -> None:
    """Main"""
    bucket_credential = f'latam-cooliving-bucket'
    [bucket_name, bob_list] = get_list_of_blobs(bucket_credential) # this bob list comes as a subtring that starts with "raw/" and ends with ".parquet"

    gcp_credentials_block_name = f'latam-cooliving-service-user'
    project_name = get_project_name(gcp_credentials_block_name)

    gcs_bigquerywarehouse_block_name = f'latam-cooliving-bqwarehouse'
    dataset_name = f'raw_latam_cooliving'
    for bob in bob_list:
        try:
            r = load_cloud_storage_to_bigquery(gcs_bigquerywarehouse_block_name, project_name, dataset_name, bucket_name, relative_file_path = bob, )
            prGreen(f'{datetime.datetime.now()} | OK ({r}) | New table available at: {bob}')
        except Exception as e:
            raise Exception(f'{datetime.datetime.now()} | ERROR | It was not possible to Create the table because: {e}')

    return

### Main
if __name__ == "__main__":
    main()









