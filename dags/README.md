### Docker Instructions
- Run ```docker-compose build``` to create the Docker image of how to build the container
    - Note: may take up to 2 minutes since we are using PySpark dependency
- Run ```docker-compose up -d``` to create the container, assuming Docker is already running, and start the services in the image
- Note: Can delete / end the services in the container using ```docker-compose down -v```

### Airflow
- The Airflow UI will run on ```http://localhost://8080```
- username / password should both be admin (for simplicity)
- Click the arrow under actions to run each job (will specify dates for when to ingest)
- If you click to trigger a dag under Actions, then it will automatically unpause the DAG, however if a DAG is paused (such as ```eia_processing```) and another DAG tirggers it (such as ```eia_ingest```), then the calls to ```eia_processing``` will be queued but not run until it is unpaused
- Incremental parameter determines whether the DAG should check in S3 if the data for this time period is already there. If you want a complete ingestion regardless if the data from the selected time frame is already there, set incremental to ```false```, otherwise it is automatically set to ```true```
- Each of the dags runs a Python script (see ```eia_ingest_core.py```, ```petroleum_coal_electricity_core.py```, or ```natural_gas_crude_core.py```) that has the same functionality as each of the Jupyter Notebooks
- Note: Some of the ingestion jobs could take some time, especially ```ingest_total_energy``` so you can end the job early by clicking on the running circle under Runs and the delete the job
- Note: ```eia_processing``` DAG may have issues when running the ```process_petroleum_coal_electricity``` task since it is reading a lot of data. Airflow automatically reruns the task and normally fixes the issue and note that the task in ```up_for_retry``` cell is redundant
