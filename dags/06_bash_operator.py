"""
DAG 6: BashOperator
Este DAG muestra cómo ejecutar comandos bash
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'estudiante',
    'retries': 1,
}

with DAG(
    dag_id='06_bash_operator',
    default_args=default_args,
    description='DAG que usa BashOperator para ejecutar comandos',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'bash'],
) as dag:
    
    mostrar_fecha = BashOperator(
        task_id='mostrar_fecha',
        bash_command='date',
    )
    
    listar_archivos = BashOperator(
        task_id='listar_archivos',
        bash_command='ls -la /tmp',
    )
    
    crear_archivo = BashOperator(
        task_id='crear_archivo',
        bash_command='echo "Hola desde Airflow" > /tmp/airflow_test.txt',
    )
    
    leer_archivo = BashOperator(
        task_id='leer_archivo',
        bash_command='cat /tmp/airflow_test.txt',
    )
    
    limpiar = BashOperator(
        task_id='limpiar',
        bash_command='rm -f /tmp/airflow_test.txt',
    )
    
    mostrar_fecha >> listar_archivos >> crear_archivo >> leer_archivo >> limpiar
