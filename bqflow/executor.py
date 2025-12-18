"""
BQFlow BigQuery SQL Query Abstraction Module
Provides parameterized query building and flexible output options:
- Native GCS parquet export
- Pandas DataFrame
- Polars DataFrame  
- PySpark DataFrame
"""

from typing import Dict, List, Optional, Union, Any
from google.cloud import bigquery
from google.cloud import storage
import time


class BigQueryExecutor:
    """
    Executes parameterized BigQuery queries with flexible output options.
    
    Supports:
    - Native GCS export (no memory overhead)
    - Pandas DataFrames
    - Polars DataFrames (lazy or eager)
    - PySpark DataFrames
    
    Example:
        executor = BigQueryExecutor(project_id='my-project')
        
        query_config = {
            'columns': ['user_id', 'name', 'email'],
            'table': 'project.dataset.users',
            'where': {'status': 'active', 'created_date >': '2024-01-01'},
            'order_by': ['created_date DESC'],
            'limit': 1000
        }
        
        # Export to GCS
        executor.export_to_gcs(
            query_config=query_config,
            gcs_uri='gs://my-bucket/exports/users-*.parquet'
        )
        
        # Or get as DataFrame
        df_pandas = executor.to_pandas(query_config)
        df_polars = executor.to_polars(query_config)
        df_spark = executor.to_spark(query_config, spark_session)
    """
    
    def __init__(self, project_id: Optional[str] = None):
        """
        Initialize BigQuery and Storage clients.
        
        Args:
            project_id: GCP project ID. If None, uses default from environment.
        """
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)
        self.project_id = project_id or self.bq_client.project
    
    def build_query(self, query_config: Dict[str, Any]) -> str:
        """
        Build SQL query from configuration dictionary.
        
        Args:
            query_config: Dictionary containing:
                - raw_sql: Raw SQL query string (bypasses other options)
                - ctes: List of CTE configurations (optional)
                - columns: List of column names or dict mapping aliases
                - table: Main table name (required if not using raw_sql)
                - joins: List of join configurations (optional)
                - where: Dict of where conditions (optional)
                - group_by: List of columns to group by (optional)
                - having: Dict of having conditions (optional)
                - order_by: List of order by clauses (optional)
                - limit: Integer limit (optional)
                - distinct: Boolean for SELECT DISTINCT (optional)
        
        Returns:
            SQL query string
        """
        # If raw SQL is provided, use it directly
        if 'raw_sql' in query_config:
            return query_config['raw_sql']
        
        parts = []
        
        # WITH clause (CTEs)
        ctes = query_config.get('ctes', [])
        if ctes:
            cte_parts = []
            for cte in ctes:
                cte_parts.append(self._format_cte(cte))
            parts.append("WITH " + ',\n'.join(cte_parts))
        
        # SELECT clause
        select = "SELECT DISTINCT" if query_config.get('distinct', False) else "SELECT"
        columns = self._format_columns(query_config.get('columns', ['*']))
        parts.append(f"{select} {columns}")
        
        # FROM clause
        table = query_config.get('table')
        if not table:
            raise ValueError("'table' is required in query_config")
        parts.append(f"FROM {table}")
        
        # JOIN clauses
        joins = query_config.get('joins', [])
        for join in joins:
            parts.append(self._format_join(join))
        
        # WHERE clause
        where = query_config.get('where')
        if where:
            parts.append(f"WHERE {self._format_conditions(where)}")
        
        # GROUP BY clause
        group_by = query_config.get('group_by')
        if group_by:
            parts.append(f"GROUP BY {', '.join(group_by)}")
        
        # HAVING clause
        having = query_config.get('having')
        if having:
            parts.append(f"HAVING {self._format_conditions(having)}")
        
        # ORDER BY clause
        order_by = query_config.get('order_by')
        if order_by:
            parts.append(f"ORDER BY {', '.join(order_by)}")
        
        # LIMIT clause
        limit = query_config.get('limit')
        if limit:
            parts.append(f"LIMIT {limit}")
        
        return '\n'.join(parts)
    
    def _format_columns(self, columns: Union[List[str], Dict[str, str]]) -> str:
        """Format column selection."""
        if isinstance(columns, dict):
            # Format as "column AS alias"
            return ', '.join([f"{col} AS {alias}" for col, alias in columns.items()])
        return ', '.join(columns)
    
    def _format_cte(self, cte_config: Dict[str, Any]) -> str:
        """
        Format CTE (Common Table Expression).
        
        Args:
            cte_config: Dict with 'name' and either 'query_config' or 'sql' keys
                Example 1 (using query_config):
                {
                    'name': 'active_users',
                    'query_config': {
                        'columns': ['user_id', 'name'],
                        'table': 'dataset.users',
                        'where': {'status': 'active'}
                    }
                }
                
                Example 2 (using raw SQL):
                {
                    'name': 'monthly_sales',
                    'sql': 'SELECT month, SUM(amount) as total FROM sales GROUP BY month'
                }
        """
        name = cte_config.get('name')
        if not name:
            raise ValueError("CTE requires 'name' field")
        
        # Check if raw SQL is provided
        if 'sql' in cte_config:
            cte_sql = cte_config['sql']
        elif 'query_config' in cte_config:
            # Build query from config
            cte_sql = self.build_query(cte_config['query_config'])
        else:
            raise ValueError("CTE requires either 'sql' or 'query_config' field")
        
        # Indent the CTE query for readability
        indented_sql = '\n'.join('  ' + line for line in cte_sql.split('\n'))
        
        return f"{name} AS (\n{indented_sql}\n)"
    
    def _format_join(self, join_config: Dict[str, Any]) -> str:
        """
        Format JOIN clause.
        
        Args:
            join_config: Dict with 'type', 'table', and 'on' keys
                Example: {
                    'type': 'LEFT',
                    'table': 'project.dataset.orders',
                    'on': 'users.id = orders.user_id'
                }
                
                Can also include 'alias' for table alias:
                {
                    'type': 'LEFT',
                    'table': 'project.dataset.orders',
                    'alias': 'o',
                    'on': 'u.id = o.user_id'
                }
        """
        join_type = join_config.get('type', 'INNER').upper()
        table = join_config.get('table')
        on = join_config.get('on')
        alias = join_config.get('alias')
        
        if not table or not on:
            raise ValueError("Join requires 'table' and 'on' fields")
        
        table_clause = f"{table} {alias}" if alias else table
        return f"{join_type} JOIN {table_clause} ON {on}"
    
    def _format_conditions(self, conditions: Dict[str, Any]) -> str:
        """
        Format WHERE/HAVING conditions.
        
        Args:
            conditions: Dict of column: value pairs
                Operators can be specified in multiple ways:
                
                Method 1 - Operator in key string:
                    {'column >': value}
                    {'column <=': value}
                    {'column IN': [value1, value2]}
                    {'column LIKE': pattern}
                    {'column BETWEEN': [start, end]}
                
                Method 2 - Dict value with operator:
                    {'column': {'operator': '>', 'value': 100}}
                    {'column': {'operator': 'IN', 'value': ['a', 'b', 'c']}}
                    {'column': {'operator': 'BETWEEN', 'value': [1, 10]}}
                
                Method 3 - Simple equality (default):
                    {'column': value}
                
                Special values:
                    {'column': None}  -> "column IS NULL"
        
        Returns:
            SQL condition string
        """
        clauses = []
        
        for key, value in conditions.items():
            # Method 2: Dict value with explicit operator
            if isinstance(value, dict) and 'operator' in value:
                operator = value['operator'].upper()
                val = value['value']
                clauses.append(self._format_condition_with_operator(key, operator, val))
            
            # Method 1: Check for operator in key string
            elif any(op in key.upper() for op in ['>=', '<=', '!=', '<>', 'IN', 'LIKE', 'BETWEEN', 'NOT IN', 'IS NULL', 'IS NOT NULL']):
                # Extract column and operator from key
                # Handle >= and <= before < and > to avoid partial matches
                if '>=' in key:
                    col, _ = key.split('>=', 1)
                    clauses.append(self._format_condition_with_operator(col.strip(), '>=', value))
                elif '<=' in key:
                    col, _ = key.split('<=', 1)
                    clauses.append(self._format_condition_with_operator(col.strip(), '<=', value))
                elif '!=' in key or '<>' in key:
                    col = key.replace('!=', '').replace('<>', '').strip()
                    clauses.append(self._format_condition_with_operator(col, '!=', value))
                elif '>' in key:
                    col, _ = key.split('>', 1)
                    clauses.append(self._format_condition_with_operator(col.strip(), '>', value))
                elif '<' in key:
                    col, _ = key.split('<', 1)
                    clauses.append(self._format_condition_with_operator(col.strip(), '<', value))
                elif 'IN' in key.upper():
                    col = key.upper().replace('IN', '').strip()
                    # Check if it's NOT IN
                    if 'NOT' in key.upper():
                        col = col.replace('NOT', '').strip()
                        clauses.append(self._format_condition_with_operator(col, 'NOT IN', value))
                    else:
                        clauses.append(self._format_condition_with_operator(col, 'IN', value))
                elif 'LIKE' in key.upper():
                    col = key.upper().replace('LIKE', '').strip()
                    clauses.append(self._format_condition_with_operator(col, 'LIKE', value))
                elif 'BETWEEN' in key.upper():
                    col = key.upper().replace('BETWEEN', '').strip()
                    clauses.append(self._format_condition_with_operator(col, 'BETWEEN', value))
                elif 'IS NULL' in key.upper():
                    col = key.upper().replace('IS NULL', '').strip()
                    clauses.append(f"{col} IS NULL")
                elif 'IS NOT NULL' in key.upper():
                    col = key.upper().replace('IS NOT NULL', '').strip()
                    clauses.append(f"{col} IS NOT NULL")
            
            # Method 3: Default to equality
            else:
                if value is None:
                    clauses.append(f"{key} IS NULL")
                else:
                    clauses.append(f"{key} = {self._format_value(value)}")
        
        return ' AND '.join(clauses)
    
    def _format_condition_with_operator(self, column: str, operator: str, value: Any) -> str:
        """
        Format a single condition with an operator.
        
        Args:
            column: Column name
            operator: Comparison operator (>, <, >=, <=, IN, BETWEEN, etc.)
            value: Value(s) to compare
        
        Returns:
            Formatted condition string
        """
        operator = operator.upper()
        
        if operator == 'IN' or operator == 'NOT IN':
            if not isinstance(value, (list, tuple)):
                value = [value]
            formatted_values = ', '.join([self._format_value(v) for v in value])
            return f"{column} {operator} ({formatted_values})"
        
        elif operator == 'BETWEEN':
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise ValueError(f"BETWEEN requires exactly 2 values, got: {value}")
            return f"{column} BETWEEN {self._format_value(value[0])} AND {self._format_value(value[1])}"
        
        elif operator in ('>', '<', '>=', '<=', '!=', '<>', 'LIKE', '='):
            return f"{column} {operator} {self._format_value(value)}"
        
        else:
            raise ValueError(f"Unsupported operator: {operator}")
    
    def _format_value(self, value: Any) -> str:
        """Format value for SQL query."""
        if isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, (list, tuple)):
            formatted = [self._format_value(v) for v in value]
            return f"({', '.join(formatted)})"
        elif value is None:
            return "NULL"
        else:
            return str(value)
    
    def build_export_query(
        self,
        query_config: Dict[str, Any],
        gcs_uri: str,
        compression: str = 'SNAPPY',
        overwrite: bool = True
    ) -> str:
        """
        Build EXPORT DATA query for BigQuery native export.
        
        Args:
            query_config: Query configuration dictionary
            gcs_uri: GCS URI (e.g., 'gs://bucket/path/file-*.parquet')
                     Use * for automatic sharding
            compression: Compression type ('SNAPPY', 'GZIP', 'NONE')
            overwrite: Whether to overwrite existing files
        
        Returns:
            EXPORT DATA SQL query string
        """
        # Build the SELECT query
        select_query = self.build_query(query_config)
        
        # Build EXPORT DATA statement
        export_parts = [
            "EXPORT DATA",
            f"OPTIONS(",
            f"  uri = '{gcs_uri}',",
            f"  format = 'PARQUET',",
            f"  compression = '{compression.upper()}',",
            f"  overwrite = {str(overwrite).lower()}",
            f") AS"
        ]
        
        # Combine export statement with query
        export_query = '\n'.join(export_parts) + '\n' + select_query
        return export_query
    
    def execute_query(
        self,
        query_config: Dict[str, Any],
        return_results: bool = False
    ) -> Optional[bigquery.table.RowIterator]:
        """
        Execute parameterized query and optionally return results.
        
        Args:
            query_config: Query configuration dictionary
            return_results: If True, returns query results
        
        Returns:
            Query results if return_results=True, else None
        """
        query = self.build_query(query_config)
        print(f"Executing query:\n{query}\n")
        
        query_job = self.bq_client.query(query)
        
        if return_results:
            results = query_job.result()
            print(f"Query completed, returned {results.total_rows} rows")
            return results
        else:
            # Just wait for completion
            query_job.result()
            print("Query completed")
            return None
    
    def export_to_gcs(
        self,
        query_config: Dict[str, Any],
        gcs_uri: str,
        compression: str = 'SNAPPY',
        overwrite: bool = True,
        wait_for_completion: bool = True
    ) -> bigquery.QueryJob:
        """
        Export query results to GCS as parquet using BigQuery native EXPORT DATA.
        
        Args:
            query_config: Query configuration dictionary
            gcs_uri: GCS URI (e.g., 'gs://bucket/path/file-*.parquet')
                     Use * for automatic sharding into multiple files
                     Use specific name for single file (works for small datasets)
            compression: Compression type ('SNAPPY', 'GZIP', 'NONE')
            overwrite: Whether to overwrite existing files
            wait_for_completion: If True, waits for export to complete
        
        Returns:
            BigQuery QueryJob object
        """
        export_query = self.build_export_query(
            query_config=query_config,
            gcs_uri=gcs_uri,
            compression=compression,
            overwrite=overwrite
        )
        
        print(f"Executing export:\n{export_query}\n")
        
        query_job = self.bq_client.query(export_query)
        
        if wait_for_completion:
            query_job.result()  # Wait for completion
            print(f"Export completed successfully to {gcs_uri}")
            print(f"Total bytes processed: {query_job.total_bytes_processed:,}")
        else:
            print(f"Export job started: {query_job.job_id}")
        
        return query_job
    
    def export_to_gcs_via_temp_table(
        self,
        query_config: Dict[str, Any],
        gcs_uri: str,
        dataset_id: str,
        temp_table_id: Optional[str] = None,
        compression: str = 'SNAPPY',
        overwrite: bool = True,
        cleanup_temp: bool = True
    ) -> str:
        """
        Export query results via temporary table for better control.
        Useful for very large exports or when you need to verify before export.
        
        Args:
            query_config: Query configuration dictionary
            gcs_uri: GCS URI for export
            dataset_id: Dataset ID for temporary table
            temp_table_id: Temporary table name (auto-generated if None)
            compression: Compression type
            overwrite: Whether to overwrite existing files
            cleanup_temp: Whether to delete temp table after export
        
        Returns:
            GCS URI of exported data
        """
        # Generate temp table name if not provided
        if temp_table_id is None:
            import uuid
            temp_table_id = f"temp_export_{uuid.uuid4().hex[:8]}"
        
        temp_table_ref = f"{self.project_id}.{dataset_id}.{temp_table_id}"
        
        try:
            # Step 1: Create temp table from query
            print(f"Creating temporary table: {temp_table_ref}")
            query = self.build_query(query_config)
            
            job_config = bigquery.QueryJobConfig(
                destination=temp_table_ref,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            
            query_job = self.bq_client.query(query, job_config=job_config)
            query_job.result()  # Wait for completion
            
            # Get row count
            table = self.bq_client.get_table(temp_table_ref)
            print(f"Temporary table created with {table.num_rows:,} rows")
            
            # Step 2: Export temp table to GCS
            print(f"Exporting to {gcs_uri}")
            
            export_config = {
                'columns': ['*'],
                'table': temp_table_ref
            }
            
            export_job = self.export_to_gcs(
                query_config=export_config,
                gcs_uri=gcs_uri,
                compression=compression,
                overwrite=overwrite
            )
            
            return gcs_uri
            
        finally:
            # Step 3: Cleanup temp table
            if cleanup_temp:
                try:
                    self.bq_client.delete_table(temp_table_ref)
                    print(f"Temporary table {temp_table_ref} deleted")
                except Exception as e:
                    print(f"Warning: Could not delete temp table: {e}")
    
    def list_exported_files(self, gcs_uri: str) -> List[str]:
        """
        List all files created by an export operation.
        
        Args:
            gcs_uri: GCS URI pattern used for export
        
        Returns:
            List of full GCS URIs of created files
        """
        # Parse GCS URI
        if not gcs_uri.startswith('gs://'):
            raise ValueError("gcs_uri must start with 'gs://'")
        
        parts = gcs_uri[5:].split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1].replace('*', '') if len(parts) > 1 else ''
        
        # List blobs
        bucket = self.storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        
        files = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]
        
        if files:
            print(f"Found {len(files)} exported files:")
            for f in files:
                print(f"  - {f}")
        else:
            print("No files found")
        
        return files


# Convenience function
def query_to_parquet(
    query_config: Dict[str, Any],
    gcs_uri: str,
    project_id: Optional[str] = None,
    compression: str = 'SNAPPY',
    overwrite: bool = True
) -> bigquery.QueryJob:
    """
    Convenience function to execute query and export to parquet.
    
    Args:
        query_config: Query configuration dictionary
        gcs_uri: GCS URI for export
        project_id: GCP project ID
        compression: Compression type ('SNAPPY', 'GZIP', 'NONE')
        overwrite: Whether to overwrite existing files
    
    Returns:
        BigQuery QueryJob object
    """
    executor = BigQueryExecutor(project_id=project_id)
    return executor.export_to_gcs(
        query_config=query_config,
        gcs_uri=gcs_uri,
        compression=compression,
        overwrite=overwrite
    )


if __name__ == "__main__":
    # Example usage
    executor = BigQueryExecutor(project_id='my-project')
    
    # Example 1: Simple query export
    simple_config = {
        'columns': ['user_id', 'name', 'email', 'created_date'],
        'table': 'my-project.my_dataset.users',
        'where': {
            'status': 'active',
            'created_date >': '2024-01-01'
        },
        'order_by': ['created_date DESC'],
        'limit': 1000
    }
    
    # Export with automatic sharding (use * for multiple files)
    # executor.export_to_gcs(
    #     query_config=simple_config,
    #     gcs_uri='gs://my-bucket/exports/users-*.parquet',
    #     compression='SNAPPY'
    # )
    
    # Example 2: Demonstrating all comparison operators
    operators_config = {
        'columns': ['order_id', 'user_id', 'amount', 'status', 'order_date'],
        'table': 'my-project.my_dataset.orders',
        'where': {
            # Method 1: Operator in key string
            'amount >': 100,                    # Greater than
            'amount <=': 5000,                  # Less than or equal
            'status IN': ['pending', 'processing', 'completed'],  # IN operator
            'order_date >=': '2024-01-01',     # Greater than or equal
            'quantity <': 10,                   # Less than
            'discount !=': 0,                   # Not equal
            
            # Method 2: Dict value with operator (alternative syntax)
            'priority': {'operator': 'IN', 'value': ['high', 'urgent']},
            'created_at': {'operator': '>=', 'value': '2024-06-01'},
            'updated_at': {'operator': '<', 'value': '2024-12-31'},
            'category': {'operator': 'NOT IN', 'value': ['archived', 'deleted']},
            'price': {'operator': 'BETWEEN', 'value': [50, 500]},
            
            # Simple equality (default)
            'is_active': True
        },
        'order_by': ['order_date DESC']
    }
    
    # executor.export_to_gcs(
    #     query_config=operators_config,
    #     gcs_uri='gs://my-bucket/exports/filtered_orders-*.parquet'
    # )
    
    # Example 3: Query with multiple joins
    multi_join_config = {
        'columns': {
            'u.user_id': 'user_id',
            'u.name': 'user_name',
            'u.email': 'email',
            'COUNT(DISTINCT o.order_id)': 'order_count',
            'SUM(o.amount)': 'total_spent',
            'MAX(o.order_date)': 'last_order_date',
            'COUNT(DISTINCT p.product_id)': 'unique_products'
        },
        'table': 'my-project.my_dataset.users u',
        'joins': [
            {
                'type': 'LEFT',
                'table': 'my-project.my_dataset.orders',
                'alias': 'o',
                'on': 'u.user_id = o.user_id'
            },
            {
                'type': 'LEFT',
                'table': 'my-project.my_dataset.order_items',
                'alias': 'oi',
                'on': 'o.order_id = oi.order_id'
            },
            {
                'type': 'LEFT',
                'table': 'my-project.my_dataset.products',
                'alias': 'p',
                'on': 'oi.product_id = p.product_id'
            }
        ],
        'where': {
            'u.status': 'active',
            'o.order_date >=': '2024-01-01',
            'o.order_date <': '2025-01-01',
            'o.amount >': 0
        },
        'group_by': ['u.user_id', 'u.name', 'u.email'],
        'having': {
            'COUNT(DISTINCT o.order_id) >': 0,
            'SUM(o.amount) >=': 100
        },
        'order_by': ['total_spent DESC']
    }
    
    # executor.export_to_gcs(
    #     query_config=multi_join_config,
    #     gcs_uri='gs://my-bucket/exports/user_analytics-*.parquet'
    # )
    
    # Example 4: Query with CTEs (Common Table Expressions)
    cte_config = {
        'ctes': [
            {
                'name': 'active_users',
                'query_config': {
                    'columns': ['user_id', 'name', 'email', 'registration_date'],
                    'table': 'my-project.my_dataset.users',
                    'where': {
                        'status': 'active',
                        'registration_date >=': '2023-01-01'
                    }
                }
            },
            {
                'name': 'user_orders',
                'query_config': {
                    'columns': {
                        'user_id': 'user_id',
                        'COUNT(order_id)': 'order_count',
                        'SUM(amount)': 'total_amount',
                        'AVG(amount)': 'avg_order_value'
                    },
                    'table': 'my-project.my_dataset.orders',
                    'where': {
                        'order_date >=': '2024-01-01',
                        'status IN': ['completed', 'shipped']
                    },
                    'group_by': ['user_id']
                }
            },
            {
                'name': 'high_value_customers',
                'sql': '''
                SELECT 
                    user_id,
                    order_count,
                    total_amount
                FROM user_orders
                WHERE total_amount > 1000
                '''
            }
        ],
        'columns': {
            'au.user_id': 'user_id',
            'au.name': 'customer_name',
            'au.email': 'email',
            'au.registration_date': 'registration_date',
            'hvc.order_count': 'total_orders',
            'hvc.total_amount': 'lifetime_value',
            'ROUND(hvc.total_amount / hvc.order_count, 2)': 'avg_order_value'
        },
        'table': 'active_users au',
        'joins': [
            {
                'type': 'INNER',
                'table': 'high_value_customers',
                'alias': 'hvc',
                'on': 'au.user_id = hvc.user_id'
            }
        ],
        'order_by': ['lifetime_value DESC']
    }
    
    # executor.export_to_gcs(
    #     query_config=cte_config,
    #     gcs_uri='gs://my-bucket/exports/high_value_customers-*.parquet'
    # )
    
    # Example 5: Complex nested CTEs with window functions
    complex_cte_config = {
        'ctes': [
            {
                'name': 'daily_sales',
                'sql': '''
                SELECT 
                    DATE(order_date) as sale_date,
                    product_id,
                    SUM(quantity) as daily_quantity,
                    SUM(amount) as daily_revenue
                FROM my-project.my_dataset.orders
                WHERE order_date >= '2024-01-01'
                GROUP BY DATE(order_date), product_id
                '''
            },
            {
                'name': 'sales_with_moving_avg',
                'sql': '''
                SELECT 
                    sale_date,
                    product_id,
                    daily_revenue,
                    AVG(daily_revenue) OVER (
                        PARTITION BY product_id 
                        ORDER BY sale_date 
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) as moving_avg_7day,
                    ROW_NUMBER() OVER (
                        PARTITION BY product_id 
                        ORDER BY daily_revenue DESC
                    ) as revenue_rank
                FROM daily_sales
                '''
            }
        ],
        'columns': [
            's.*',
            'p.product_name',
            'p.category'
        ],
        'table': 'sales_with_moving_avg s',
        'joins': [
            {
                'type': 'LEFT',
                'table': 'my-project.my_dataset.products',
                'alias': 'p',
                'on': 's.product_id = p.product_id'
            }
        ],
        'where': {
            's.revenue_rank <=': 10,
            's.daily_revenue >': 0
        },
        'order_by': ['s.product_id', 's.sale_date']
    }
    
    # executor.export_to_gcs(
    #     query_config=complex_cte_config,
    #     gcs_uri='gs://my-bucket/exports/product_analytics-*.parquet'
    # )
    
    # Example 6: Using raw SQL for maximum flexibility
    raw_sql_config = {
        'raw_sql': '''
        WITH RECURSIVE date_range AS (
            SELECT DATE('2024-01-01') as date
            UNION ALL
            SELECT DATE_ADD(date, INTERVAL 1 DAY)
            FROM date_range
            WHERE date < '2024-12-31'
        ),
        orders_by_date AS (
            SELECT 
                DATE(order_date) as order_date,
                COUNT(*) as order_count,
                SUM(amount) as revenue
            FROM my-project.my_dataset.orders
            WHERE status IN ('completed', 'shipped')
                AND amount > 0
            GROUP BY DATE(order_date)
        )
        SELECT 
            dr.date,
            COALESCE(obd.order_count, 0) as orders,
            COALESCE(obd.revenue, 0) as revenue,
            SUM(COALESCE(obd.revenue, 0)) OVER (
                ORDER BY dr.date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as cumulative_revenue
        FROM date_range dr
        LEFT JOIN orders_by_date obd ON dr.date = obd.order_date
        ORDER BY dr.date
        '''
    }
    
    # executor.export_to_gcs(
    #     query_config=raw_sql_config,
    #     gcs_uri='gs://my-bucket/exports/daily_revenue_report-*.parquet'
    # )
    
    # List exported files
    # files = executor.list_exported_files('gs://my-bucket/exports/users-*.parquet')