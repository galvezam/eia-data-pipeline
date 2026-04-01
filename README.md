### EIA Data Pipeline
Register for an API here: ...

### Ingestion
- Note that the jupyter notebook currently has ingestion for the year of 2025. We will add more data for previous years to get that amount of data greater than 2GB. We will run a batch job to get data from the previous months for the data that allows it (some data only comes annually), so that we don't overload the API and get rate limited.
- The target_data should be around 6 GB and can be downloaded at https://www.eia.gov/opendata/v1/bulkfiles.php, although we want more granular data so we will use the API.
- We will add processing layers with Polars in the weeks to come and a dashboard to visualize the data, either in time series graphs or on a map of the US.
