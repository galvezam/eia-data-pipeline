### EIA Data Pipeline
- Register for an API here: https://www.eia.gov/opendata/

### Dependencies
- Python environment assumes Airflow, Docker, PySpark, Boto3, and Streamlit are already included in the environment
- Google Colab is used for running the Ingestion and Processing Jupyter Notebooks manually.
- Create the virtual enviroment with the necessary dependencies in ```requirements.txt```
    - ```python -m venv my_env```
    - ```source my_env/bin/activate```
    - ```pip install -r requirements.txt```


### Airflow & Docker
- In the terminal, create the Docker image with ```docker-compose build```
- Then, run ```docker-compose up -d``` to spin up the container
- Airflow UI is found in ```http://localhost:8080```
- Username and password are both ```admin```
- By defualt, both DAGs are paused. Unpause ```eia_ingest``` and ```eia_processing``` on the left hand side
- Run the airflow dag by triggering the ```eia_ingest``` DAG under Actions on the right hand side
    - Note: The ingestion layer can take some time (> 5 minutes) for longer date ranges. We recommend not going back farther than a month.
    - The incremental option is only if you don't want to ingest data with the selected time frame that is already in the S3 bucket. It is automatically set to ```true``` but if you would like to see the data ingested in S3, set it to ```false```
- All details for running Airflow and Docker can be found [here](./dags/README.md)

### Ingestion
- API Keys and other secrets should be configured in Google Colab Secrets on the left hand side of the Google Colab notebook under the key icon
- Note that the jupyter notebook currently has ingestion for the year of 2025. We will add more data for previous years to ideally get that amount of data greater than 2GB. We will run a batch job to get data from the previous months for the data that allows it (some data only comes annually), so that we don't overload the API and get rate limited.
- The persistence with getting API rate limited is sporadic so if you are having issues, try running on a smaller time frame
- The target_data should be around 15 GB and can be downloaded at https://www.eia.gov/opendata/v1/bulkfiles.php, although we will use the API in this project. All of the bulk files can be found under ```target_data/``` in the AWS S3 bucket

#### Running the ingestion layer
- To manually run the ingestion layer, we suggest using the jupyter notebook in ```ingest/total_ingest.ipynb``` as you can see the data directly.
- You can also use airflow, following the instructions in ```dags/README.md``` but you won't be able to see the data itself.

### Processing

#### Running the processing layer
- API Keys and other secrets should be configured in Google Colab Secrets on the left hand side of the Google Colab notebook under the key icon
- To manually run the processing layer, we suggest using the jupyter notebook in either ```processing/natural_gas_crude_processing.ipynb``` or ```processing/petroleum_coal_electricity_total.ipynb``` as you can see the data directly.
- Can also follow the instructions in ```dags/README.md``` but you won't be able to see the data itself.


### Dashboard
- Start the dashboard using ```streamlit run dashboard/app.py```.
- ```dashboard/data_loader.py``` loads and caches the data from AWS S3.
- Further details are available [here](./dashboard/README.md)
