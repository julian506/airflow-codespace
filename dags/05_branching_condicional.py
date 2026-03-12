"""
DAG 5: Branching - Ejecución Condicional
Este DAG muestra cómo ejecutar diferentes tareas según condiciones
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

default_args = {
    'owner': 'estudiante',
    'retries': 1,
}

def verificar_dia(**context):
    """Decide qué rama ejecutar según el día de la semana"""
    dia_semana = context['data_interval_end'].weekday()
    print(f"Día de la semana: {dia_semana}")
    
    if dia_semana < 5:  # Lunes a Viernes (0-4)
        return 'tarea_dia_laboral'
    else:  # Sábado y Domingo (5-6)
        return 'tarea_fin_semana'

def procesar_dia_laboral():
    print("Procesando datos de día laboral")

def procesar_fin_semana():
    print("Procesando datos de fin de semana")

def finalizar():
    print("Proceso finalizado")

with DAG(
    dag_id='05_branching_condicional',
    default_args=default_args,
    description='DAG con ejecución condicional (branching)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'branching'],
    params = {
        default_args['owner']: 'estudiante',
    }
) as dag:
    
    verificar = BranchPythonOperator(
        task_id='verificar_dia',
        python_callable=verificar_dia,
    )
    
    dia_laboral = PythonOperator(
        task_id='tarea_dia_laboral',
        python_callable=procesar_dia_laboral,
    )
    
    fin_semana = PythonOperator(
        task_id='tarea_fin_semana',
        python_callable=procesar_fin_semana,
    )
    
    fin = PythonOperator(
        task_id='finalizar',
        python_callable=finalizar,
        trigger_rule='none_failed_min_one_success',
    )
    
    verificar >> [dia_laboral, fin_semana] >> fin
