### Docker Instructions
- Run ```docker-compose build``` to create the Docker image of how to build the container
    - Note: may take up to 2 minutes since we are using PySpark dependency
- Run ```docker-compose up -d``` to create the container, assuming Docker is already running, and start the services in the image
- Note: Can delete / end the services in the container using ```docker-compose down -v```

### Airflow
- The Airflow UI will run on localhost://8080
- username / password should both be admin (just for simplicity)
- Click the arrow under actions to run each job (will specify dates for when to ingest)
- The incremental option is only for we have already run jobs and will only need to update recent data (but this is not necessary for this demo)
- Each of the dags runs a Python script (see ```eia_ingest_core.py```, ```petroleum_coal_electricity_core.py```, or ```natural_gas_crude_core.py```) that has the same functionality as each of the Jupyter Notebooks
- Note: Some of the ingestion jobs could take some time, especially ```ingest_total_energy``` so you can end the job early by clicking on the running circle under Runs and the delete the job.