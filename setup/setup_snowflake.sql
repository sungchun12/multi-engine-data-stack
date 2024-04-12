CREATE DATABASE TEST_ICEBERG_SNOWFLAKE_CATALOG;
CREATE SCHEMA TEST_ICEBERG_SNOWFLAKE_CATALOG.TEST_SCHEMA;

--------------------------------
-- CREATE EXTERNAL VOLUME
--------------------------------
CREATE OR REPLACE EXTERNAL VOLUME exbucketmultiengine
   STORAGE_LOCATIONS =
      (
         (
            NAME = '-bucket'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = ''
            STORAGE_AWS_ROLE_ARN = ''
         )
      );

DESC EXTERNAL VOLUME exbucketmultiengine;

--------------------------------
-- CREATE CATALOG INTEGRATION
--------------------------------
CREATE OR REPLACE CATALOG INTEGRATION glueCatalogInt
   CATALOG_SOURCE=GLUE
   CATALOG_NAMESPACE='multiengine'
   TABLE_FORMAT=ICEBERG
   GLUE_AWS_ROLE_ARN=''
   GLUE_CATALOG_ID=''
   GLUE_REGION=''
   ENABLED=TRUE;
;
DESC CATALOG INTEGRATION glueCatalogInt;

--------------------------------
-- CREATE STAGING TABLE
--------------------------------
CREATE ICEBERG TABLE IF NOT EXISTS staging_taxi
  EXTERNAL_VOLUME='exbucketmultiengine'
  CATALOG='glueCatalogInt'
  CATALOG_TABLE_NAME='staging_taxi';

SELECT count(*) from staging_taxi; -- 10
ALTER ICEBERG TABLE staging_taxi REFRESH;
SELECT count(*) from staging_taxi; 

---



CREATE OR REPLACE ICEBERG TABLE TEST_ICEBERG_SNOWFLAKE_CATALOG.TEST_SCHEMA.TAXI_STATING_PREDICTED 
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'exbucketmultiengine'
  BASE_LOCATION = 'staging'
  AS SELECT * FROM myGlueTable;

SELECT * FROM TAXI_STATING_PREDICTED;   
select count(*) from TAXI_STATING_PREDICTED;

SELECT top 10 * FROM TAXI_STATING_PREDICTED;   



