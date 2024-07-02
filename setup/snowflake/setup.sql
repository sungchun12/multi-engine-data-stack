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
CREATE OR REPLACE ICEBERG TABLE MULTIENGINE_DB.REVIEWS.landing_reviews
  EXTERNAL_VOLUME='exbucketmultiengine'
  CATALOG='glueCatalogIntMultiengine'
  CATALOG_TABLE_NAME='landing_reviews';


