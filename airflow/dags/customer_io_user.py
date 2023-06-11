from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift import (
    RedshiftCreateTableOperator,
    RedshiftDeleteTableOperator,
)
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.operators.dummy import DummyOperator

# DAG configurations
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# S3 bucket and key
s3_bucket = 'analytics'
s3_key = 'path/to/file.parquet'

# Redshift settings
redshift_conn_id = 'your_redshift_connection_id'
redshift_table = 'customer_io_user'
redshift_schema = 'public'
redshift_create_table_query = '''
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        column1 VARCHAR(255),
        column2 INT,
        column3 VARCHAR(255)
    )
'''

def recreate_redshift_table():
    """Recreate Redshift table if the S3 file is updated."""
    redshift_create_table = RedshiftCreateTableOperator(
        task_id='redshift_create_table',
        redshift_conn_id=redshift_conn_id,
        sql=redshift_create_table_query.format(schema=redshift_schema, table=redshift_table),
    )
    redshift_delete_table = RedshiftDeleteTableOperator(
        task_id='redshift_delete_table',
        redshift_conn_id=redshift_conn_id,
        table=redshift_table,
        schema=redshift_schema,
    )
    redshift_create_table.execute(context=None)
    redshift_delete_table.execute(context=None)

with DAG('validate_s3_file', default_args=default_args, schedule_interval=timedelta(minutes=5)) as dag:

    s3_file_sensor = S3KeySensor(
        task_id='s3_file_sensor',
        bucket_name=s3_bucket,
        bucket_key=s3_key,
        wildcard_match=False,
        aws_conn_id='aws_default',
    )

    recreate_redshift_table = PythonOperator(
        task_id='recreate_redshift_table',
        python_callable=recreate_redshift_table,
    )

    s3_file_sensor >> recreate_redshift_table
