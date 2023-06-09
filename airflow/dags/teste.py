from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

def extract_data():
    # Conexão com o MySQL
    mysql_task = MySqlOperator(
        task_id='extract_data',
        mysql_conn_id='mysql_default',
        sql="SELECT * FROM user; SELECT * FROM account;",
        dag=dag
    )
    mysql_task.execute(context={})

def save_to_json():
    # Função para salvar os dados em um arquivo JSON
    def _save_to_json(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='extract_data')
        data = {'tabela1': result[0], 'tabela2': result[1]}
        with open('/path/to/output.json', 'w') as f:
            json.dump(data, f)
    
    # Tarefa Python para salvar os dados em um arquivo JSON
    python_task = PythonOperator(
        task_id='save_to_json',
        python_callable=_save_to_json,
        provide_context=True,
        dag=dag
    )

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'extrair_dados_mysql',
    default_args=default_args,
    schedule='*/5 * * * *'
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

save_task = PythonOperator(
    task_id='save_to_json',
    python_callable=save_to_json,
    dag=dag
)

extract_task >> save_task