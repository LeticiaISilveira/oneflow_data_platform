from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from kafka import KafkaProducer
import pymysql
import boto3
from avro import schema, datafile, io
import zipfile
import io

# DAG configurations
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the S3 bucket name
s3_bucket = 'your_bucket'

# Define Kafka settings
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'topic_name'

# Define MySQL settings
mysql_host = 'localhost'
mysql_user = 'your_user'
mysql_password = 'your_password'
mysql_database = 'your_database'

# Global variable to store previous table metadata versions
last_table_metadata = {}

def serialize_avro(data):
    """Serialize data into Avro format.

    Args:
        data (dict): Data to be serialized.

    Returns:
        bytes: Serialized Avro data.
    """
    avro_schema = schema.Parse('''
        {
            "type": "record",
            "name": "TableName",
            "fields": [
                {"name": "column1", "type": "string"},
                {"name": "column2", "type": "int"},
                {"name": "column3", "type": "string"}
            ]
        }
    ''')

    writer = io.DatumWriter(avro_schema)
    avro_bytes_writer = io.BytesIO()
    avro_file_writer = datafile.DataFileWriter(avro_bytes_writer, writer, avro_schema)
    avro_file_writer.append(data)
    avro_file_writer.close()
    return avro_bytes_writer.getvalue()

def check_mysql_updates(table):
    """Check if there were updates in the MySQL table.

    Args:
        table (str): Name of the table to check.

    Returns:
        bool: True if there were updates, False otherwise.
    """
    global last_table_metadata

    mysql_connection = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        db=mysql_database
    )

    cursor = mysql_connection.cursor()
    cursor.execute(f'SHOW CREATE TABLE {table}')
    table_metadata = cursor.fetchone()[1]

    if table not in last_table_metadata or table_metadata != last_table_metadata[table]:
        last_table_metadata[table] = table_metadata
        return True
    else:
        return False

def send_data_to_kafka(table):
    """Send data to Kafka.

    Args:
        table (str): Name of the table to send data from.
    """
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    mysql_connection = pymysql.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        db=mysql_database
    )

    cursor = mysql_connection.cursor()
    cursor.execute(f'SELECT * FROM {table}')

    for row in cursor:
        data = {
            "column1": row[0],
            "column2": row[1],
            "column3": row[2]
        }

        serialized_data = serialize_avro(data)
        producer.send(kafka_topic, value=serialized_data)

    cursor.close()
    mysql_connection.close()
    producer.flush()

def persist_data_to_s3(table, execution_date):
    """Persist data to Amazon S3.

    Args:
        table (str): Name of the table to persist data from.
        execution_date (datetime): Execution date of the DAG.
    """
    s3_client = boto3.client('s3')

    prefix = f"raw/{execution_date.year}/{execution_date.month}/{execution_date.day}/{table}"
    in_memory_zip = io.BytesIO()

    with zipfile.ZipFile(in_memory_zip, 'w') as zf:
        for message in consumer:
            serialized_data = message.value
            s3_key = f"{prefix}/{message.timestamp}.avro"
            zf.writestr(s3_key, serialized_data)

    in_memory_zip.seek(0)
    s3_key = f"{prefix}/data.zip"
    s3_client.upload_fileobj(in_memory_zip, s3_bucket, s3_key)

with DAG('my_pipeline', default_args=default_args, schedule_interval=timedelta(minutes=5)) as dag:

    tables = ['user', 'account']  # Define the tables to be checked

    task_check_mysql_updates = []
    task_send_data_to_kafka = []
    task_persist_data_to_s3 = []

    for table in tables:
        task_check_mysql_update = PythonOperator(
            task_id=f'check_mysql_updates_{table}',
            python_callable=check_mysql_updates,
            op_args=[table]
        )
        task_check_mysql_updates.append(task_check_mysql_update)

        task_send_data_to_kafka = PythonOperator(
            task_id=f'send_data_to_kafka_{table}',
            python_callable=send_data_to_kafka,
            op_args=[table],
            trigger_rule='one_success'
        )
        task_send_data_to_kafka.append(task_send_data_to_kafka)

        task_persist_data_to_s3 = PythonOperator(
            task_id=f'persist_data_to_s3_{table}',
            python_callable=persist_data_to_s3,
            op_args=[table, '{{ execution_date }}'],
            trigger_rule='one_success'
        )
        task_persist_data_to_s3.append(task_persist_data_to_s3)

        task_check_mysql_updates[-1] >> task_send_data_to_kafka[-1] >> task_persist_data_to_s3[-1]

    task_no_update = DummyOperator(task_id='no_update')
    task_finish = DummyOperator(task_id='finish')

    for task_check_mysql_update in task_check_mysql_updates:
        task_check_mysql_update >> task_no_update >> task_finish

    for task_persist_data_to_s3 in task_persist_data_to_s3:
        task_check_mysql_update >> task_send_data_to_kafka >> task_persist_data_to_s3 >> task_finish
