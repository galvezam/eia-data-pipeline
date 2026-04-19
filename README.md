### EIA Data Pipeline
Register for an API here: https://www.eia.gov/opendata/

### Ingestion
- Note that the jupyter notebook currently has ingestion for the year of 2025. We will add more data for previous years to get that amount of data greater than 2GB. We will run a batch job to get data from the previous months for the data that allows it (some data only comes annually), so that we don't overload the API and get rate limited.
- The target_data should be around 6 GB and can be downloaded at https://www.eia.gov/opendata/v1/bulkfiles.php, although we will use the API in this project.

#### Running the ingestion layer
- To manually run the ingestion layer, we suggest using the jupyter notebook in ```ingest/total_ingest.ipynb``` as you can see the data directly.
- You can also use airflow, following the instructions in ```dags/README.md``` but you won't be able to see the data itself.

### Processing

#### Running the processing layer
- To manually run the processing layer, we suggest using the jupyter notebook in either ```processing/natural_gas_crude_processing.ipynb``` or ```processing/petroleum_coal_electricity_total.ipynb``` as you can see the data directly.
- Can also follow the instructions in ```dags/README.md``` but you won't be able to see the data itself.


### Dashboard
- Start the dashboard using ```streamlit run dashboard/app.py```.
- ```dashboard/data_loader.py``` loads and caches the data from AWS S3.
