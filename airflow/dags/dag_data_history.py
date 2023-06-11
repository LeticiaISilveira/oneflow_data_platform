from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
import subprocess

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 10),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'extract_and_save_mysql_to_s3',
    default_args=default_args,
    schedule_interval=None  # Do not schedule periodically
)

tables = ['user', 'account']  # Add the tables you want to extract

spark_submit_task_id = 'spark_submit_task'
s3_bucket = 'raw'
s3_region = 'eu-north-1'  # Replace with your desired region

def extract_and_save_table(table):
    spark = SparkSession.builder.getOrCreate()

    """
    Extract the table data from MySQL.
    """
    df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/database_name",
        driver="com.mysql.jdbc.Driver",
        dbtable=table,
        user="your_username",
        password="your_password"
    ).load()

    """
    Save the extracted data to local filesystem in Avro format.
    """
    avro_path = f'/path/to/{table}.avro'
    avro_schema = StructType([
        StructField('updated', TimestampType()),
        StructField('created', TimestampType())
    ])
    avro_options = {
        'header': 'true',
        'avroSchema': avro_schema.json()
    }
    df.select([col(c).alias(c.lower()) for c in df.columns]).write.format("avro").mode("overwrite").options(**avro_options).save(avro_path)

    """
    Compress the Avro file using gzip.
    """
    compressed_path = f'{avro_path}.gz'
    subprocess.run(['gzip', avro_path])

    """
    Save the compressed file to S3.
    """
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_key = f'{table}/{{{{ execution_date.year }}}}/{{{{ execution_date.month }}}}/{{{{ execution_date.day }}}}/{table}.avro.gz'
    s3_hook.load_file(
        filename=compressed_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )

def create_bucket():
    subprocess.run(['python', 's3_create_bucket.py'])

with dag:
    """
    Create the S3 bucket if it doesn't exist.
    """
    create_bucket_task = PythonOperator(
        task_id='create_bucket',
        python_callable=create_bucket
    )

    """
    Extract and save the tables.
    """
    for table in tables:
        """
        Extract and save each table using PySpark.
        """
        extract_and_save_task = PythonOperator(
            task_id=f'extract_and_save_{table}',
            python_callable=extract_and_save_table,
            op_args=[table]
        )

        create_bucket_task >> extract_and_save_task
