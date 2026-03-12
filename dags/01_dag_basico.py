"""
DAG 1: Conceptos Básicos
Este DAG demuestra la estructura más simple de un DAG en Airflow
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Argumentos por defecto para el DAG
default_args = {
    'owner': 'estudiante',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Función simple que se ejecutará
def saludar():
    print("¡Hola! Este es mi primer DAG de Airflow")
    return "Tarea completada"

# Definición del DAG
with DAG(
    dag_id='01_dag_basico',
    default_args=default_args,
    description='Un DAG simple para aprender los conceptos básicos',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'basico'],
) as dag:
    
    # Tarea única
    tarea_saludo = PythonOperator(
        task_id='saludar',
        python_callable=saludar,
    )
