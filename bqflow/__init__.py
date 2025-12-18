"""
BQFlow: BigQuery SQL Abstraction Package

A Python package for abstracting BigQuery SQL queries as parameterized functions
and exporting results to dataframes or Google Cloud Storage as Parquet files.

Main classes:
- BigQueryExecutor: Execute parameterized queries and export to GCS
- SQLParser: Parse SQL queries into config dictionaries

Example:
    from bqflow import BigQueryExecutor, sql_to_config
    
    # Using SQL parser
    sql = "SELECT * FROM users WHERE status = 'active'"
    config = sql_to_config(sql)
    
    # Export to GCS
    executor = BigQueryExecutor(project_id='my-project')
    executor.export_to_gcs(
        query_config=config,
        gcs_uri='gs://my-bucket/exports/users-*.parquet'
    )
"""

from .executor import BigQueryExecutor, query_to_parquet
from .parser import SQLParser, sql_to_config
from .__version__ import __version__, __author__, __description__

__all__ = [
    'BigQueryExecutor',
    'query_to_parquet',
    'SQLParser',
    'sql_to_config',
    '__version__',
    '__author__',
    '__description__',
]