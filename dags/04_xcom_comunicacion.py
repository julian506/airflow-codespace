"""
DAG 4: XCom - Comunicación entre Tareas
Este DAG muestra cómo compartir datos entre tareas usando XCom
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'estudiante',
    'retries': 1,
}

def generar_numero(**context):
    """Genera un número y lo guarda en XCom"""
    numero = 42
    print(f"Generando número: {numero}")
    # El return automáticamente guarda el valor en XCom
    return numero

def multiplicar_numero(**context):
    """Obtiene el número de XCom y lo multiplica"""
    # Obtener el valor de la tarea anterior usando XCom
    ti = context['ti']
    numero = ti.xcom_pull(task_ids='generar_numero')
    resultado = numero * 2
    print(f"Número recibido: {numero}, Resultado: {resultado}")
    return resultado

def mostrar_resultado(**context):
    """Muestra el resultado final"""
    ti = context['ti']
    resultado = ti.xcom_pull(task_ids='multiplicar_numero')
    print(f"El resultado final es: {resultado}")

with DAG(
    dag_id='04_xcom_comunicacion',
    default_args=default_args,
    description='DAG que demuestra comunicación entre tareas con XCom',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'xcom'],
) as dag:
    
    tarea_generar = PythonOperator(
        task_id='generar_numero',
        python_callable=generar_numero,
    )
    
    tarea_multiplicar = PythonOperator(
        task_id='multiplicar_numero',
        python_callable=multiplicar_numero,
    )
    
    tarea_mostrar = PythonOperator(
        task_id='mostrar_resultado',
        python_callable=mostrar_resultado,
    )
    
    tarea_generar >> tarea_multiplicar >> tarea_mostrar
