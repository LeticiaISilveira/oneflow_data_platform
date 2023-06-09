# Importe as bibliotecas necessárias
import airflow
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Verifique se a conexão MySQL foi configurada corretamente
try:
    # Crie uma instância da classe MySqlHook passando o nome da conexão como argumento
    hook = MySqlHook(mysql_conn_id='mysql_default')

    # conn = hook.get_conn()
    # Execute uma consulta de teste usando o método get_records()
    result = hook.get_records("SELECT * FROM user LIMIT 1")

    # Verifique se os resultados foram retornados corretamente
    if result:
        print("Conexão com o MySQL bem-sucedida!")
        print("Resultado da consulta:")
        print(result)
    else:
        print("A consulta não retornou resultados.")
except Exception as e:
    print("Erro ao conectar ao MySQL:", str(e))
    