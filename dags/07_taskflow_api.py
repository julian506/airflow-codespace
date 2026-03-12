"""
DAG 7: TaskFlow API
Este DAG muestra la sintaxis moderna de Airflow usando decoradores
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'estudiante',
    'retries': 1,
}

@dag(
    dag_id='07_taskflow_api',
    default_args=default_args,
    description='DAG usando TaskFlow API (decoradores)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'taskflow'],
)
def tutorial_taskflow():
    """
    DAG usando la API moderna de Airflow con decoradores.
    Los datos se pasan automáticamente entre tareas.
    """
    
    @task
    def obtener_datos():
        """Simula la obtención de datos"""
        datos = {
            'usuarios': 150,
            'ventas': 3500,
            'productos': 45
        }
        print(f"Datos obtenidos: {datos}")
        return datos
    
    @task
    def procesar_datos(datos: dict):
        """Procesa los datos recibidos"""
        datos['promedio_venta'] = datos['ventas'] / datos['usuarios']
        print(f"Datos procesados: {datos}")
        return datos
    
    @task
    def generar_reporte(datos: dict):
        """Genera un reporte con los datos"""
        reporte = f"""
        === REPORTE DIARIO ===
        Usuarios: {datos['usuarios']}
        Ventas: ${datos['ventas']}
        Productos: {datos['productos']}
        Promedio por usuario: ${datos['promedio_venta']:.2f}
        """
        print(reporte)
        return "Reporte generado exitosamente"
    
    # Definir el flujo de datos
    datos = obtener_datos()
    datos_procesados = procesar_datos(datos)
    generar_reporte(datos_procesados)

# Instanciar el DAG
tutorial_taskflow()
