"""
DAG 8: Procesamiento de Datos con PokeAPI y Pandas
Este DAG obtiene datos de la PokeAPI, los procesa con pandas y genera análisis
"""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
import requests
from typing import List, Dict

default_args = {
    'owner': 'estudiante',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='08_pokeapi_pandas',
    default_args=default_args,
    description='DAG que procesa datos de Pokemon usando PokeAPI y Pandas',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tutorial', 'api', 'pandas'],
)
def pokeapi_etl():
    """
    Pipeline ETL que:
    1. Extrae datos de Pokemon desde la PokeAPI
    2. Transforma los datos usando pandas
    3. Genera estadísticas y análisis
    """
    
    @task
    def obtener_lista_pokemon(limite: int = 50) -> List[str]:
        """
        Obtiene la lista de nombres de Pokemon desde la API
        """
        print(f"Obteniendo lista de {limite} Pokemon...")
        url = f"https://pokeapi.co/api/v2/pokemon?limit={limite}"
        
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        pokemon_names = [pokemon['name'] for pokemon in data['results']]
        
        print(f"Se obtuvieron {len(pokemon_names)} Pokemon")
        return pokemon_names
    
    @task
    def obtener_detalles_pokemon(pokemon_names: List[str]) -> List[Dict]:
        """
        Obtiene los detalles de cada Pokemon
        """
        print(f"Obteniendo detalles de {len(pokemon_names)} Pokemon...")
        pokemon_data = []
        
        for name in pokemon_names[:20]:  # Limitamos a 20 para no saturar la API
            try:
                url = f"https://pokeapi.co/api/v2/pokemon/{name}"
                response = requests.get(url)
                response.raise_for_status()
                
                data = response.json()
                
                # Extraer información relevante
                pokemon_info = {
                    'name': data['name'],
                    'id': data['id'],
                    'height': data['height'],
                    'weight': data['weight'],
                    'base_experience': data['base_experience'],
                    'hp': data['stats'][0]['base_stat'],
                    'attack': data['stats'][1]['base_stat'],
                    'defense': data['stats'][2]['base_stat'],
                    'special_attack': data['stats'][3]['base_stat'],
                    'special_defense': data['stats'][4]['base_stat'],
                    'speed': data['stats'][5]['base_stat'],
                    'type_1': data['types'][0]['type']['name'],
                    'type_2': data['types'][1]['type']['name'] if len(data['types']) > 1 else None,
                }
                
                pokemon_data.append(pokemon_info)
                print(f"✓ {name.capitalize()}")
                
            except Exception as e:
                print(f"✗ Error obteniendo {name}: {str(e)}")
                continue
        
        print(f"Se obtuvieron detalles de {len(pokemon_data)} Pokemon")
        return pokemon_data
    
    @task
    def crear_dataframe(pokemon_data: List[Dict]) -> str:
        """
        Crea un DataFrame de pandas con los datos de Pokemon
        """
        print("Creando DataFrame con pandas...")
        df = pd.DataFrame(pokemon_data)
        
        print(f"\nDataFrame creado con {len(df)} filas y {len(df.columns)} columnas")
        print("\nPrimeras filas:")
        print(df.head())
        
        print("\nInformación del DataFrame:")
        print(df.info())
        
        # Guardar como CSV (en producción usarías un storage real)
        csv_path = '/tmp/pokemon_data.csv'
        df.to_csv(csv_path, index=False)
        print(f"\nDatos guardados en: {csv_path}")
        
        return csv_path
    
    @task
    def analizar_estadisticas(csv_path: str) -> Dict:
        """
        Realiza análisis estadísticos sobre los datos de Pokemon
        """
        print("Analizando estadísticas de Pokemon...")
        df = pd.read_csv(csv_path)
        
        # Calcular estadísticas
        stats = {
            'total_pokemon': len(df),
            'promedio_hp': df['hp'].mean(),
            'promedio_attack': df['attack'].mean(),
            'promedio_defense': df['defense'].mean(),
            'pokemon_mas_fuerte': df.loc[df['attack'].idxmax(), 'name'],
            'pokemon_mas_resistente': df.loc[df['defense'].idxmax(), 'name'],
            'pokemon_mas_rapido': df.loc[df['speed'].idxmax(), 'name'],
        }
        
        print("\n=== ESTADÍSTICAS DE POKEMON ===")
        print(f"Total de Pokemon analizados: {stats['total_pokemon']}")
        print(f"HP promedio: {stats['promedio_hp']:.2f}")
        print(f"Ataque promedio: {stats['promedio_attack']:.2f}")
        print(f"Defensa promedio: {stats['promedio_defense']:.2f}")
        print(f"Pokemon más fuerte: {stats['pokemon_mas_fuerte'].capitalize()}")
        print(f"Pokemon más resistente: {stats['pokemon_mas_resistente'].capitalize()}")
        print(f"Pokemon más rápido: {stats['pokemon_mas_rapido'].capitalize()}")
        
        return stats
    
    @task
    def analizar_tipos(csv_path: str) -> Dict:
        """
        Analiza la distribución de tipos de Pokemon
        """
        print("Analizando tipos de Pokemon...")
        df = pd.read_csv(csv_path)
        
        # Contar tipos
        tipos_count = df['type_1'].value_counts()
        
        print("\n=== DISTRIBUCIÓN DE TIPOS ===")
        print(tipos_count)
        
        tipo_mas_comun = tipos_count.index[0]
        cantidad = tipos_count.iloc[0]
        
        print(f"\nTipo más común: {tipo_mas_comun.capitalize()} ({cantidad} Pokemon)")
        
        return {
            'tipo_mas_comun': tipo_mas_comun,
            'cantidad': int(cantidad),
            'distribucion': tipos_count.to_dict()
        }
    
    @task
    def generar_reporte_final(stats: Dict, tipos: Dict):
        """
        Genera un reporte final consolidado
        """
        print("\n" + "="*50)
        print("REPORTE FINAL - ANÁLISIS DE POKEMON")
        print("="*50)
        
        print(f"\n📊 Total de Pokemon: {stats['total_pokemon']}")
        
        print("\n🏆 Campeones:")
        print(f"  • Más fuerte: {stats['pokemon_mas_fuerte'].capitalize()}")
        print(f"  • Más resistente: {stats['pokemon_mas_resistente'].capitalize()}")
        print(f"  • Más rápido: {stats['pokemon_mas_rapido'].capitalize()}")
        
        print("\n📈 Promedios:")
        print(f"  • HP: {stats['promedio_hp']:.2f}")
        print(f"  • Ataque: {stats['promedio_attack']:.2f}")
        print(f"  • Defensa: {stats['promedio_defense']:.2f}")
        
        print(f"\n🎨 Tipo más común: {tipos['tipo_mas_comun'].capitalize()} ({tipos['cantidad']} Pokemon)")
        
        print("\n" + "="*50)
        print("✅ Análisis completado exitosamente")
        print("="*50)
    
    # Definir el flujo del pipeline
    pokemon_names = obtener_lista_pokemon(limite=50)
    pokemon_details = obtener_detalles_pokemon(pokemon_names)
    csv_file = crear_dataframe(pokemon_details)
    
    # Análisis en paralelo
    stats = analizar_estadisticas(csv_file)
    tipos = analizar_tipos(csv_file)
    
    # Reporte final
    generar_reporte_final(stats, tipos)

# Instanciar el DAG
pokeapi_etl()
