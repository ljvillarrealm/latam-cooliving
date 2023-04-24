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
@task(name="scrap_ListOfLanguages.py : Get the data", log_prints=True, retries=3)
def scrap_data(url: str, attrrbs: dict, read_flavor: str, table_pos: int) -> pd.DataFrame:
    """Scrap the data"""
    data = pd.read_html(url, attrs=attrrbs, flavor=read_flavor)
    df = data[table_pos]

    return df

@task(name="scrap_ListOfLanguages.py : Clean the data", log_prints=True)
def clean_data(df: pd.DataFrame) -> pd.DataFrame :
    """Clean the data"""
    expected_col = 6
    expected_row = 34 # Curently, there are 34 countries in Latin American and the Caribbean
    # Validate columns
    if (df.shape[1] == expected_col):
        df.columns = ['country', 'official_language', 'regional_laguage', 'minority_language', 'national_language', 'widely_spoken']
    else:
        raise Exception(f'{datetime.datetime.now()} | ERROR | The scrapped columns are {df.shape[1]}. This does not match the expected [{expected_col}].')
    # Validate rows
    if (df.shape[0] < expected_row): 
        raise Exception(f'{datetime.datetime.now()} | ERROR | The scrapped rows are {df.shape[0]}. This does not match the expected [>{expected_row}].')
    # Clean
    df['country'] = df['country'].str.replace('\d', '', regex=True).str.replace('\W', '', regex=True)
    df['language'] = df['official_language'].str.split(' ').str[0].replace('\d', '', regex=True).str.replace('\W', '', regex=True)
    df.loc[df.country.str.upper() == 'Argentina'.upper(), 'language'] = f'Spanish'
    df.loc[(df.country).str.upper() == 'Chile'.upper(), 'language'] = f'Spanish'
    df.loc[(df.country).str.upper() == 'Peru'.upper(), 'language'] = f'Spanish'
    df.loc[(df.country).str.upper() == 'Mexico'.upper(), 'language'] = f'Spanish'
    df.loc[(df.country).str.upper() == 'Uruguay'.upper(), 'language'] = f'Spanish'
    df.loc[(df.country).str.upper() == 'Aruba'.upper(), 'language'] = f'Dutch'
    df.loc[(df.country).str.upper() == 'None'.upper(), 'language'] = f'Unknown'
    df.loc[(df.country).str.upper() == 'Null'.upper(), 'language'] = f'Unknown'
    df.loc[(df.country).isnull(), 'language'] = f'Unknown'
    df.loc[(df.country).isna(), 'language'] = f'Unknown'
    # Columns
    df['ingestion_time'] = datetime.datetime.now()
    # Reorder columns
    df_out = df[['ingestion_time', 'country', 'language']].copy()

    return df_out

@task(name="scrap_ListOfLanguages.py : write df to local as parquet", log_prints=True)
def write_local_as_parquet(df: pd.DataFrame, local_path: str, dataset_file: str) -> Path : 
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{local_path}{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(name="scrap_ListOfLanguages.py : load parquet to cloud", log_prints=True, retries=3)
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
@flow(name="scrap_ListOfLanguages.py : Main", log_prints=True)
def main() -> None:
    """Main"""
    local_path = f'{Path.home()}/latam-cooliving/dataflows/data/'
    url = f'https://en.wikipedia.org/wiki/List_of_official_languages_by_country_and_territory'
    attributes = {'class': 'wikitable sortable'}
    flavor = f'bs4'
    table_nro = 0
    bucket_credential = f'latam-cooliving-bucket'

    # scrap
    df = scrap_data(url, attributes, flavor, table_nro)
    # clean
    df = clean_data(df)
    # to parquet
    p_df = write_local_as_parquet(df, local_path, dataset_file='languages')
    # to cloud
    write_cloud_gcs(bucket_credential, p_df)

    prGreen(f'{datetime.datetime.now()} | OK | New file loaded: {p_df}')

    return

### Main
if __name__ == "__main__":
    main()