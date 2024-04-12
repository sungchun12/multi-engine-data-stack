from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq
import os 

os.environ["AWS_REGION"] = "eu-central-1"

## CREATE CATALOG
catalog = load_catalog("glue")

## CREATE SCHEMA
catalog.create_namespace("multiengine")

# CREATE TABLE
catalog.create_table(
    "multiengine.landing_taxi",
    schema=pq.read_table("/tmp/yellow_tripdata_2023-01.parquet").schema,
    location="s3:///landing/taxi",
)

catalog.create_table(
    "multiengine.staging_taxi",
    schema=pq.read_table("/tmp/yellow_tripdata_2023-01.parquet").schema,
    location="s3:///staging/taxi",
)