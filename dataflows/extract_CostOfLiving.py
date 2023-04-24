### Start
# Libraries
import os
import datetime
import pandas as pd
from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

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
@task(name="extract_CostOfLiving.py : Download the kaggle data", log_prints=True, retries=3)
def download_kaggle_data(kaggle_path: str, kaggle_dataset: str, kaggle_file: str) -> pd.DataFrame:
    """Download the kaggle data"""
    r = os.system(f'kaggle datasets download -d {kaggle_dataset} -p {kaggle_path} --unzip') #r = kaggle.api.datasets_download(owner_slug=f"mvieira101", dataset_slug=f"global-cost-of-living", async_req=False)
    if (r==0):
        df = pd.read_csv(f'{kaggle_path}{kaggle_file}', encoding='utf-8')
    else:
        raise Exception(f'{datetime.datetime.now()} | ERROR | It was not possible to download the data from kaggle because.')
    
    return df

@task(name="extract_CostOfLiving.py : Get the custom kaggle data dictionary", log_prints=True, retries=3)
def import_datadic(file_csv: str) -> pd.DataFrame:
    """Get the custom kaggle data dictionary"""
    df = pd.read_csv(file_csv)
    
    #raise Exception(f'{datetime.datetime.now()} | ERROR | It was not possible to get the DataDic.')
    return df

@task(name="extract_CostOfLiving.py : Clean the kaggle data", log_prints=True)
def clean_kaggle_data(df: pd.DataFrame) -> pd.DataFrame :
    """Clean the kaggle data"""
    expected_col=58
    # Validate columns
    if (df.shape[1] >= expected_col):
        df_m = pd.melt(df, id_vars=['city', 'country', 'data_quality'], var_name='item', value_name='amount')
    else:
        raise Exception(f'{datetime.datetime.now()} | ERROR | The columns are {df.shape[1]}. This does not match the expected [{expected_col}].')
    # Validate rows
    if (df_m.shape[0] <= 0):
        raise Exception(f'{datetime.datetime.now()} | ERROR | The number of extracted rows is <=0.')
    # Columns
    df_m['ingestion_time'] = datetime.datetime.now()
    # Reorder columns
    df_out = df_m[['ingestion_time', 'country', 'city', 'item', 'amount', 'data_quality']]

    return df_out

@task(name="extract_CostOfLiving.py : Clean the dic data", log_prints=True)
def clean_dic_data(df: pd.DataFrame) -> pd.DataFrame :
    """Clean the dictionary data"""
    expected_row=55
    # Validate rows
    if (df.shape[0] <= expected_row):
        raise Exception(f'{datetime.datetime.now()} | ERROR | The number of extracted rows is <={expected_row}.')
    # Columns
    df['ingestion_time'] = datetime.datetime.now()
    # Reorder columns
    df_out = df[['ingestion_time', 'item', 'category', 'description']]

    return df_out

@task(name="extract_CostOfLiving.py : write df to local as parquet", log_prints=True)
def write_local_as_parquet(df: pd.DataFrame, local_path: str, dataset_file: str) -> Path : 
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{local_path}{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(name="extract_CostOfLiving.py : load parquet to cloud", log_prints=True, retries=3)
def write_cloud_gcs(bucket_credential: str, file_path: Path) -> None : 
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load(bucket_credential)
    
    #print(file_path)
    #print(file_path.home())
    #print(file_path.absolute())

    file_name = str(file_path).split('/')[-1]
    bucket_path = f'{file_name}'
    #prGreen(file_name)
    #prGreen(bucket_path)
    
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path = file_path
        , to_path = bucket_path
    )

    return


### Flows
@flow(name="extract_CostOfLiving.py : Main", log_prints=True)
def main() -> None:
    """Main"""
    local_path = f'{Path.home()}/latam-cooliving/dataflows/data/'
    kaggle_path = f'data/kaggle/'
    kaggle_dataset = f'mvieira101/global-cost-of-living'
    kaggle_file = f'cost-of-living_v2.csv'
    dic_file = f'{Path.home()}/latam-cooliving/data/DataDictionary.csv'
    bucket_credential = f'latam-cooliving-bucket'

    # get
    df_liv = download_kaggle_data(kaggle_path, kaggle_dataset, kaggle_file)
    df_dic = import_datadic(dic_file)
    # clean
    df_liv = clean_kaggle_data(df_liv)
    df_dic = clean_dic_data(df_dic)
    # to parquet
    p_liv = write_local_as_parquet(df_liv, local_path, dataset_file='cost_of_living')
    p_dic = write_local_as_parquet(df_dic, local_path, dataset_file='cost_of_living_dic')
    # to cloud
    write_cloud_gcs(bucket_credential, p_liv)
    write_cloud_gcs(bucket_credential, p_dic)

    prGreen(f'{datetime.datetime.now()} | OK | New file loaded: {p_liv}')
    prGreen(f'{datetime.datetime.now()} | OK | New file loaded: {p_dic}')

    return

### Main
if __name__ == "__main__":
    main()