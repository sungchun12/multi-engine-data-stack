from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq
from dagster import asset, EnvVar
from random import randint 
from dagster_snowflake import SnowflakeResource
import os 

os.environ["AWS_REGION"] = "eu-central-1"

snowflake = SnowflakeResource(
    account="",  
    user="", 
    password="",  
    warehouse="",
    schema="",
    database="",
    role="",
)

@asset
def generate_taxi_data():
    df = pq.read_table("/tmp/yellow_tripdata_2023-01.parquet")
    catalog = load_catalog(name="glue")
    table = catalog.load_table("multiengine.landing_taxi")
    batch = 1000
    first_row = randint(1, df.shape[0]-batch)
    table.append(df[first_row:first_row+batch])


@asset(deps=["generate_taxi_data"])
def deduplicate_taxi_data():
    catalog = load_catalog(name="glue")

    # load landing data in duckdb
    con = catalog.load_table("multiengine.landing_taxi").scan().to_duckdb(table_name="landing_taxi")

    # deduplicate data
    output = con.execute("""
    SELECT * FROM landing_taxi
    QUALIFY
        row_number() OVER (PARTITION BY TPEP_PICKUP_DATETIME, TPEP_DROPOFF_DATETIME) =1;
    """).arrow()

    # write in iceberg staging table
    catalog.load_table("multiengine.staging_taxi").overwrite(output)

    # Create Snowflake Iceberg table if needed 
    query = """
        CREATE ICEBERG TABLE IF NOT EXISTS staging_taxi
        EXTERNAL_VOLUME='exbucketmultiengine'
        CATALOG='glueCatalogIntMultiengine'
        CATALOG_TABLE_NAME='staging_taxi';
    """
    with snowflake.get_connection() as conn:
        conn.cursor().execute(query)

    # refresh snowflake iceberg table
    query = """
        ALTER ICEBERG TABLE staging_taxi REFRESH;
    """
    with snowflake.get_connection() as conn:
        conn.cursor().execute(query)



@asset(deps=["deduplicate_taxi_data"])
def predict_sf():
    query = """
      CREATE OR REPLACE ICEBERG TABLE TEST_ICEBERG_SNOWFLAKE_CATALOG.TEST_SCHEMA.STAGING_TAXI_PREDICTION 
      CATALOG = 'SNOWFLAKE'
      EXTERNAL_VOLUME = 'exbucketmultiengine'
      BASE_LOCATION = 'staging_taxi_prediction'
      AS SELECT 
        *,
        1 as prediction
      FROM STAGING_TAXI;
    """
    with snowflake.get_connection() as conn:
        conn.cursor().execute(query)