from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# List of BigQuery tables in the dataset
TABLES = [
    'Customers',
    'Subscriptions'
]

def check_and_load(table_name, **kwargs):
    """
    Check if the table has either a created_at or updated_at column.
    If so, query for rows where these timestamps are within the last 60 minutes.
    """
    # Create a BigQueryHook (assumes gcp_conn_id is set in Airflow)
    bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default', use_legacy_sql=False)
    dataset = 'sandbox'
    
    # Query the INFORMATION_SCHEMA to get the list of columns for this table
    sql_check = f"""
        SELECT column_name 
        FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table_name}'
    """
    results = bq_hook.get_records(sql_check)
    columns = [r[0] for r in results]

    # Check for the columns we are interested in
    has_created = 'created_at' in columns
    has_updated = 'updated_at' in columns
    
    if not (has_created or has_updated):
        print(f"Table {table_name} has no created_at or updated_at column; skipping incremental load.")
        return

    # Build the incremental condition: only rows updated or created within the last 60 minutes.
    conditions = []
    if has_created:
        conditions.append("created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)")
    if has_updated:
        conditions.append("updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)")
    
    # If both exist, we want rows that satisfy either condition.
    where_clause = " OR ".join(conditions)
    
    # Build the query that returns the new records
    sql_query = f"SELECT * FROM `{dataset}.{table_name}` WHERE {where_clause}"
    print(f"Executing query for {table_name}: {sql_query}")
    
    # Execute the query. (In production you might use this result to load to another table.)
    new_records = bq_hook.get_records(sql_query)
    print(f"Found {len(new_records)} new records in {table_name}.")
    
    # Here you would include any additional logic to load these records incrementally

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG. Here we schedule it to run hourly.
with DAG(
    dag_id='incremental_load_dag',
    default_args=default_args,
    description='Incrementally load records from BigQuery tables based on created_at/updated_at',
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # For each table, create a PythonOperator that will check for new records
    for table in TABLES:
        task = PythonOperator(
            task_id=f'incremental_load_{table}',
            python_callable=check_and_load,
            op_kwargs={'table_name': table},
        )
