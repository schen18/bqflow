"""
# BQFlow: BigQuery SQL Abstraction Package

A Python package for abstracting BigQuery SQL queries as parameterized functions with flexible output options:
- Native BigQuery EXPORT DATA to GCS as Parquet (zero memory overhead)
- Pandas DataFrames
- Polars DataFrames (eager or lazy)
- PySpark DataFrames
- PyArrow Tables

## Features

- **Parameterized Query Building**: Build complex SQL queries using Python dictionaries
- **SQL Parser**: Convert existing SQL queries to config format
- **Multiple Output Formats**: Export to GCS or load into DataFrames
- **Native BigQuery Export**: Use BigQuery's EXPORT DATA for optimal performance
- **CTE Support**: Handle Common Table Expressions (WITH clauses)
- **Multiple Joins**: Support for complex multi-table joins
- **All SQL Operators**: Support for >, <, >=, <=, IN, BETWEEN, LIKE, etc.
- **Framework Agnostic**: Works seamlessly with Pandas, Polars, and PySpark

## Installation

Basic installation:
```bash
pip install bqflow
```

With DataFrame support:
```bash
# For Pandas
pip install bqflow[pandas]

# For Polars
pip install bqflow[polars]

# For PySpark
pip install bqflow[spark]

# For all DataFrame libraries
pip install bqflow[all]
```

Or install from source:
```bash
git clone https://github.com/yourusername/bqflow.git
cd bqflow
pip install -e .
```

## Quick Start

### Export to GCS (No Memory Overhead)

```python
from bqflow import BigQueryExecutor

executor = BigQueryExecutor(project_id='my-project')

query_config = {
    'columns': ['user_id', 'name', 'email'],
    'table': 'project.dataset.users',
    'where': {'status': 'active'},
    'limit': 1000
}

# Export directly to GCS as Parquet
executor.export_to_gcs(
    query_config=query_config,
    gcs_uri='gs://my-bucket/exports/users-*.parquet'
)
```

### Load into DataFrames

```python
# Pandas
df_pandas = executor.to_pandas(query_config)
print(df_pandas.head())

# Polars (eager)
df_polars = executor.to_polars(query_config)
print(df_polars.head())

# Polars (lazy)
lf = executor.to_polars(query_config, lazy=True)
result = lf.filter(pl.col('age') > 18).collect()

# PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df_spark = executor.to_spark(query_config, spark_session=spark)
df_spark.show()

# PyArrow
arrow_table = executor.to_arrow(query_config)
print(arrow_table.schema)

# Generic interface
df = executor.to_dataframe(query_config, framework='pandas')
df = executor.to_dataframe(query_config, framework='polars', lazy=True)
df = executor.to_dataframe(query_config, framework='spark', spark_session=spark)
```

### Using SQL Parser

```python
from bqflow import sql_to_config

sql = '''
SELECT u.user_id, u.name, COUNT(o.order_id) as order_count
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
WHERE u.status = 'active' AND o.amount > 100
GROUP BY u.user_id, u.name
ORDER BY order_count DESC
'''

config = sql_to_config(sql)

# Export to GCS or load into DataFrame
executor.export_to_gcs(config, gcs_uri='gs://bucket/data-*.parquet')
# OR
df = executor.to_polars(config)
```

## Integration Examples

### With Polars Pipeline

```python
import polars as pl

# Get data from BigQuery into Polars
df = executor.to_polars({
    'columns': ['order_id', 'user_id', 'amount', 'order_date'],
    'table': 'orders',
    'where': {'order_date >=': '2024-01-01'}
})

# Continue with Polars operations
result = (
    df.filter(pl.col('amount') > 100)
      .group_by('user_id')
      .agg([
          pl.col('order_id').count().alias('order_count'),
          pl.col('amount').sum().alias('total_spent')
      ])
      .sort('total_spent', descending=True)
)
```

### With PySpark Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

spark = SparkSession.builder \
    .appName("BigQuery ETL") \
    .config("spark.jars.packages", 
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0") \
    .getOrCreate()

# Get data from BigQuery into Spark
df = executor.to_spark({
    'columns': ['order_id', 'user_id', 'amount'],
    'table': 'orders',
    'where': {'amount >': 0}
}, spark_session=spark)

# Continue with Spark operations
result = (
    df.filter(col('amount') > 100)
      .groupBy('user_id')
      .agg(
          count('order_id').alias('order_count'),
          sum('amount').alias('total_spent')
      )
)

# Write back to GCS or BigQuery
result.write.parquet('gs://bucket/output/')
```

### With Pandas for Data Science

```python
import pandas as pd

# Get data from BigQuery
df = executor.to_pandas({
    'columns': ['user_id', 'feature1', 'feature2', 'target'],
    'table': 'ml_dataset',
    'where': {'split': 'train'}
})

# Use with scikit-learn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

X = df[['feature1', 'feature2']]
y = df['target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
model = RandomForestClassifier()
model.fit(X_train, y_train)
```

## Advanced Usage

### Complex Queries with CTEs

```python
config = {
    'ctes': [
        {
            'name': 'active_users',
            'query_config': {
                'columns': ['user_id', 'name'],
                'table': 'dataset.users',
                'where': {'status': 'active'}
            }
        },
        {
            'name': 'high_value',
            'sql': 'SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id'
        }
    ],
    'columns': ['au.*', 'hv.total'],
    'table': 'active_users au',
    'joins': [
        {'type': 'INNER', 'table': 'high_value', 'alias': 'hv', 
         'on': 'au.user_id = hv.user_id'}
    ]
}

df = executor.to_polars(config)
```

### Multiple Comparison Operators

```python
config = {
    'columns': ['*'],
    'table': 'orders',
    'where': {
        'amount >': 100,
        'amount <=': 5000,
        'status IN': ['pending', 'processing'],
        'created_at >=': '2024-01-01',
        'discount !=': 0,
        'category': {'operator': 'NOT IN', 'value': ['archived']},
        'price': {'operator': 'BETWEEN', 'value': [50, 500]}
    }
}
```

## Performance Considerations

### When to Use GCS Export vs DataFrames

**Use GCS Export when:**
- Dataset is very large (> 1GB)
- You need maximum performance
- Memory is constrained
- Data will be used by multiple processes
- You're building a data pipeline

**Use DataFrames when:**
- Dataset fits in memory (< 1GB typically)
- You need immediate data analysis
- You're doing exploratory data analysis
- You're integrating with ML libraries
- Interactive notebook work

### Optimization Tips

```python
# For large datasets, use GCS export
executor.export_to_gcs(
    query_config=large_query,
    gcs_uri='gs://bucket/data-*.parquet',  # * for sharding
    compression='SNAPPY'
)

# For medium datasets with Polars, use lazy evaluation
lf = executor.to_polars(query_config, lazy=True)
result = (
    lf.filter(pl.col('important_col').is_not_null())
      .select(['col1', 'col2'])
      .collect()  # Execute all at once
)

# For PySpark, ensure BigQuery connector is configured
df = executor.to_spark(
    query_config,
    spark_session=spark
)
```

## Requirements

- Python 3.8+
- google-cloud-bigquery>=3.11.0
- google-cloud-storage>=2.10.0
- sqlparse>=0.4.4

Optional (install as needed):
- pandas>=2.0.0
- polars>=0.19.0
- pyspark>=3.4.0
- pyarrow>=12.0.0

## Authentication

This package uses Google Cloud authentication. Set up credentials:

```bash
# Using service account
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# Or using gcloud
gcloud auth application-default login
```

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues and questions, please use the GitHub issue tracker.
"""