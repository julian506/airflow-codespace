"""
DAG 2: Dependencias Lineales
Este DAG muestra cómo crear una secuencia de tareas que se ejecutan una después de otra
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'estudiante',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extraer_datos():
    print("Paso 1: Extrayendo datos...")
    return "Datos extraídos"

def transformar_datos():
    print("Paso 2: Transformando datos...")
    return "Datos transformados"

def cargar_datos():
    print("Paso 3: Cargando datos...")
    return "Datos cargados"

with DAG(
    dag_id='02_dependencias_lineales',
    default_args=default_args,
    description='DAG que muestra dependencias lineales (ETL)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'dependencias'],
) as dag:
    
    tarea_extraer = PythonOperator(
        task_id='extraer',
        python_callable=extraer_datos,
    )
    
    tarea_transformar = PythonOperator(
        task_id='transformar',
        python_callable=transformar_datos,
    )
    
    tarea_cargar = PythonOperator(
        task_id='cargar',
        python_callable=cargar_datos,
    )
    
    # Definir el orden de ejecución
    tarea_extraer >> tarea_transformar >> tarea_cargar
