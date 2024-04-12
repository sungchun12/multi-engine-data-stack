from setuptools import find_packages, setup

setup(
    name="pipelinedagster",
    packages=find_packages(exclude=["pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-snowflake",
        "pyiceberg",
        "pyarrow",
        "pandas",
        "pyiceberg[glue]",
        "boto3",
        "duckdb"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
