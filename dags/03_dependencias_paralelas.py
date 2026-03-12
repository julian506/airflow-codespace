"""
DAG 3: Dependencias Paralelas
Este DAG demuestra cómo ejecutar múltiples tareas en paralelo
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'estudiante',
    'retries': 1,
}

def iniciar_proceso():
    print("Iniciando el proceso...")

def procesar_fuente_a():
    print("Procesando datos de la Fuente A")

def procesar_fuente_b():
    print("Procesando datos de la Fuente B")

def procesar_fuente_c():
    print("Procesando datos de la Fuente C")

def consolidar_resultados():
    print("Consolidando todos los resultados")

with DAG(
    dag_id='03_dependencias_paralelas',
    default_args=default_args,
    description='DAG con tareas que se ejecutan en paralelo',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'paralelo'],
) as dag:
    
    inicio = PythonOperator(
        task_id='inicio',
        python_callable=iniciar_proceso,
    )
    
    fuente_a = PythonOperator(
        task_id='procesar_fuente_a',
        python_callable=procesar_fuente_a,
    )
    
    fuente_b = PythonOperator(
        task_id='procesar_fuente_b',
        python_callable=procesar_fuente_b,
    )
    
    fuente_c = PythonOperator(
        task_id='procesar_fuente_c',
        python_callable=procesar_fuente_c,
    )
    
    consolidar = PythonOperator(
        task_id='consolidar',
        python_callable=consolidar_resultados,
    )
    
    # Una tarea inicial, tres en paralelo, y una final
    inicio >> [fuente_a, fuente_b, fuente_c] >> consolidar
