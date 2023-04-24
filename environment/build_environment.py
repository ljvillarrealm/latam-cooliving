### Start
# Libraries
import json
import datetime
from pathlib import Path
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse

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
def prefect_gcp_credentials_block(block_name: str, service_account_info_file_path: str) -> list[str]:
    """Create the Prefect GCP Credential Block"""
    try:
        with open(service_account_info_file_path, 'rb') as file:
            parsed_json = json.load(file)

            GcpCredentials(
                service_account_info = parsed_json
            ).save(name = block_name, overwrite = True)

        load_gcp_credentials_block = GcpCredentials.load(block_name)
        project_name = load_gcp_credentials_block.__getattribute__("project")
        
        prGreen(f'{datetime.datetime.now()} | SUCCESS | The Prefect GCP Credential Block "{block_name}" was successfully created/updated.')

    except Exception as e:
        raise Exception(f'{datetime.datetime.now()} | ERROR | It was not possible to Create the Prefect GCP Credential Block because: {e}')
    
    return [block_name, project_name]

def prefect_gcs_bucket_block(block_name: str, bucket_name: str, gcp_credential: str, bucket_folder: str) -> str:
    """Create the Prefect Gcs Bucket Block"""
    try:
        GcsBucket(
            gcp_credentials = GcpCredentials.load(name = gcp_credential)
            , bucket = bucket_name
            , bucket_folder = bucket_folder
        ).save(name = block_name, overwrite = True)

        prGreen(f'{datetime.datetime.now()} | SUCCESS | The Prefect GCP Credential Block "{block_name}" was successfully created/updated.')

    except Exception as e:
        raise Exception(f'{datetime.datetime.now()} | ERROR | It was not possible to Create the Prefect Gcs Bucket Block because: {e}')

    return block_name

def prefect_bigquery_warehouse_block(block_name: str, gcp_credential: str, fetch_size: int) -> str:
    """Create the Prefect BigQuery Warehouse Block"""
    try:
        BigQueryWarehouse(
            gcp_credentials = GcpCredentials.load(name = gcp_credential)
            , fetch_size = fetch_size
        ).save(name = block_name, overwrite = True)

        prGreen(f'{datetime.datetime.now()} | SUCCESS | The Prefect GCP Credential Block "{block_name}" was successfully created/updated.')

    except Exception as e:
        raise Exception(f'{datetime.datetime.now()} | ERROR | It was not possible to Create the Prefect Gcs Bucket Block because: {e}')

    return block_name

### Flows
def prefect_main() -> None:
    """Prefect main creates the blocks: GCP Credential, GCS Bucket."""
    gcp_credentials_block_name = f'latam-cooliving-service-user'
    gcp_credentials_service_account_info = f'{Path.home()}/latam-cooliving/credentials/gcp_service_account.json'
    [service_user_credential, project_name] = prefect_gcp_credentials_block(gcp_credentials_block_name, gcp_credentials_service_account_info)
    
    gcs_bucket_block_name = f'latam-cooliving-bucket'
    gcs_bucket_bucket_name = f'latam_cooliving_{project_name}'
    gcs_bucket_bucket_folder_name = f'raw'
    bucket_credential = prefect_gcs_bucket_block(gcs_bucket_block_name, gcs_bucket_bucket_name, service_user_credential, gcs_bucket_bucket_folder_name)

    gcs_bigquerywarehouse_block_name = f'latam-cooliving-bqwarehouse'
    fetch_size = 1
    bigquerywarehouse_credential = prefect_bigquery_warehouse_block(gcs_bigquerywarehouse_block_name, service_user_credential, fetch_size)


    prGreen(f'{datetime.datetime.now()} | OK | New GCP credential created/updated: GCP Credentials/{service_user_credential}')
    prGreen(f'{datetime.datetime.now()} | OK | New GCS Bucket credential created/updated: GCP GCS Bucket/{bucket_credential}')
    prGreen(f'{datetime.datetime.now()} | OK | New GCP BigQuery Warehouse credential created/updated: BigQuery Warehouse/{bigquerywarehouse_credential}')

    return

### Main
if __name__ == "__main__":
    prefect_main()