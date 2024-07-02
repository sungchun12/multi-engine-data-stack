from pyiceberg.catalog import load_catalog
import pyarrow.parquet as pq
import pyarrow as pa
import os 

## CREATE CATALOG
catalog = load_catalog("glue", **{"type": "glue", "s3.region":"eu-central-1"})

## CREATE SCHEMA
ns = catalog.list_namespaces()
if "multiengine" not in [n[0] for n in ns]:
    print("Creating namespace")
    catalog.create_namespace("multiengine")

tables = catalog.list_tables("multiengine")

if ("multiengine", "landing_reviews") not in tables:
    print("Creating table")
    schema = pa.schema(
        [
            pa.field("reviewid", pa.string(), nullable=False),
            pa.field("username", pa.string(), nullable=False),
            pa.field("review", pa.string(), nullable=True),
            pa.field("ingestion_date", pa.int64(), nullable=False),
        ]
    )

    catalog.create_table(
        "multiengine.landing_reviews",
        schema=schema,
        location="s3://sumeo-parquet-data-lake/landing/reviews",
    )
