# About this documentation
Taking account all the steps is a hard task.

In this manual are presented the most important steps to deploy the solution.

Please, do not hesitate to contactme for improvement proporsals.

# Repository structure

* __credentials__: The .json credential files.
* __data__: The required data for the project, including backups.
* __dataflows__: The orchestrated pipeline (written in python) in prefect.
* __dbt__: The dbt solution.
* __environment__: The required files to create the excecution environment.
* __images__: The available images for the repo.
* __terraform__: The terraform scripts for IaC deplyment.

# Prerequisites

* A local instance (Ubuntu 22.04.2 LTS is recommended):
  * git installed.
  * python installed.
  * [Terraform](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli) v1.4.5+ installed.
* A Google account and a Google Cloud Platform account. If you do not have a GCP account, [create one](https://console.cloud.google.com/freetrial/). This project can be completed using only the services included in the [GCP free tier](https://cloud.google.com/free/).
* A [Kaggle](https://www.kaggle.com/) account and API credential.
* A [dbt](https://www.getdbt.com/) cloud account.

# Preparations

1. Download this repo on the folder latam-cooliving (the home path ~/latam-cooliving is recommended).

    ![Home folder](images/home_folder.png)

2. Setup the kaggle account for using the API.
    Download the file kaggle.json and copy it to the folder ~.kaggle

    ![Kaggle folder](images/kaggle_folder.png)

    The kaggle file should look like this:

    ``` json
    {"username":"<YOUR USERNAME>","key":"46s4f56a4fd98f47da416dc98e4cd89c486c4e894da6d16a8d4a984c9a614c6sa8f49a8f74ed89f489f4c5c61as62c1a63dc"}
    ```

3. Setup the GCP account. After creating your GCP account, create or modify the following resources to enable Terraform to provision your infrastructure, Prefect access to storage and BigQuery, and dbt to perform BigQuery operations:
    * A GCP Project: GCP organizes resources into projects. Create one now in the GCP console and make note of the project ID. You can see a list of your projects in the cloud resource manager (It's recommended to create a project with a name like this: "latam-cooliving13987".).
    * A GCP service account key: Create a service account key to enable Terraform, Prefect and dbt to access your GCP account. When creating the key, use the following steps:
        * Select the project you created in the previous step.
        * Click "Create Service Account".
        * Give it any name you like and click "Create".
        * For the Role, choose the editor roles ["BigQuery Data Editor", "BigQuery Resource Editor", "Editor"], then click "Continue".

            ![GCP service roles](images/GCP_roles.png)

        * Skip granting other users access, and click "Done".

        After you create your service account, download your service account key.
        * Select your service account from the list.
        * Select the "Keys" tab.
        * In the drop down menu, select "Create new key".
        * Leave the "Key Type" as JSON.
        * Click "Create" to create the key and save the key file to your system.
        * Rename of the file to "gcp_service_account.json". The file should look like this:

            ``` json
            {
            "type": "service_account",
            "project_id": "<YOUR PROJECT ID>",
            "private_key_id": "f6sg196fwef691w589f4169wf4156ewf",
            "private_key": "-----BEGIN PRIVATE KEY-----\****hash****\n-----END PRIVATE KEY-----\n",
            "client_email": "dfs1f51sd5f61sdf@f6df4156asd41f56a4d54fd.gserviceaccount.com",
            "client_id": "6f1s5e6f1ws65f41wfe",
            "auth_uri": "https://accounts.google.com",
            "token_uri": "https://oauth2.googleapis.com",
            "auth_provider_x509_cert_url": "https://www.googleapis.com",
            "client_x509_cert_url": "https://www.googleapis.com"
            }
            ```

            __Warning !__
            __The service account key file provides access to your GCP project. It should be treated like any other secret credentials. Specifically, it should never be checked into source control.__

        * Copy the file to the credentials folder. So, the folder should like this:

            ![Credentials folder](images/Credentials.png)

    * Save the project name and use it for terraform setup:
        * Open the file variables.tf in the terraform folder. Modify the name of the project, so it should look like this:

            ![terraform variables file](images/terraform_variables.png)

            _Modify to the preffered region and the filepath to the gcp_service_account.json file with the GCP credentials_

4. Setup the dbt project:
    * Create a new project.
        * Set the subfolder option to "dbt".

            ![dbt P1](images/dbt_1.png)

        * Select the bigQuery connection using the credential in the "gcp_service_account.json" file.

            ![dbt P2](images/dbt_2.png)

            Set "dbt_latamcooliving" as the dataset name.

            ![dbt P3](images/dbt_3.png)

    * Import the dbt files (frm the dbt repo folder) so you can see the following File explorer structure:

        ![dbt P4](images/dbt_4.png)

5. Setup the local environment
    * Create a virtual environment and install the environment requirements using pip:

        ``` python
        pip install -r requirements
        ```

    * Excecute the "build_environment.py":

        ``` python
        python environment/build_environment.py 
        ```

        The following message should appear if the excecution was succesful:

        ![dbt P4](images/build_environment.png)

# Run the project

1. Deploy the terraform infrastructure:

    ``` bash
    terraform init
    terraform plan
    terraform apply
    ```

2. Run prefect server and open the [prefect UI](http://127.0.0.1:4200):

    ``` python
    prefect server start
    ```

3. Run the prefect dataflow (using the created environment):

    ``` python
    python dataflows/orch_flow.py
    ```

    If the excution ends ok, the should appear a meesage like this:

    ![prefect flow](images/prefect_flow_results.png)

4. Excecute dbt run

    ``` bash
    dbt run --var 'is_test: false'
    ```

5. (optional) At this point, the data shuold be available to create the dashboard:

    ![BQ data 1](images/BQ_data_1.png)

    ![BQ data 2](images/BQ_data_2.png)

6. (optional) Destroy the terraform infrastructure:

    ``` bash
    terraform destroy
    ```

