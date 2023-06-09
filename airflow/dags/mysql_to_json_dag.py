from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'lis',
    'start_date': datetime(2023, 6, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

mysql_conn_id = 'mysql_default'

def mysql_to_json():
    # Conectar ao MySQL
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    # Executar a consulta SQL
    sql = "SELECT * FROM user"
    cursor.execute(sql)
    result = cursor.fetchall()

    # Salvar os resultados em um arquivo JSON
    output_file = 'airflow\teste\dados.json'
    with open(output_file, 'w') as file:
        json.dump(result, file)

    # Fechar a conexão com o MySQL
    cursor.close()
    connection.close()

with DAG('mysql_to_json_dag', default_args=default_args, schedule='*/5 * * * *') as dag:
    task_mysql_to_json = PythonOperator(
        task_id='mysql_to_json_task',
        python_callable=mysql_to_json
    )