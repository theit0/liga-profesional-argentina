from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import time
import os

# Configuración de la API
API_KEY = os.getenv('API_FOOTBALL_KEY', 'your_api_key_here')
BASE_URL = 'https://v3.football.api-sports.io'
HEADERS = {
    'X-RapidAPI-Key': API_KEY,
    'X-RapidAPI-Host': 'v3.football.api-sports.io'
}

# ID de la Liga Profesional Argentina
LEAGUE_ID = 128
SEASONS = [2021, 2022, 2023]

def make_api_request(url, params=None):
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        time.sleep(1)  # evitar rate limit
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error en API request: {e}")
        raise

def extract_fixtures(**context):
    all_fixtures = []
    for season in SEASONS:
        url = f"{BASE_URL}/fixtures"
        params = {'league': LEAGUE_ID, 'season': season}
        data = make_api_request(url, params)
        if data['response']:
            for fixture in data['response']:
                all_fixtures.append({
                    'season': season,
                    'league_id': LEAGUE_ID,
                    'league_name': 'Liga Profesional Argentina',
                    'fixture_id': fixture['fixture']['id'],
                    'date_utc': fixture['fixture']['date'],
                    'round': fixture['league']['round'],
                    'home_id': fixture['teams']['home']['id'],
                    'home_name': fixture['teams']['home']['name'],
                    'away_id': fixture['teams']['away']['id'],
                    'away_name': fixture['teams']['away']['name'],
                    'goals_home': fixture['goals']['home'],
                    'goals_away': fixture['goals']['away'],
                })
    df = pd.DataFrame(all_fixtures)
    df.to_csv('/tmp/fixtures_base.csv', index=False)
    return len(all_fixtures)

def extract_standings_data(**context):
    all_standings = []
    for season in SEASONS:
        print(f"Extrayendo standings para temporada {season}")
        url = f"{BASE_URL}/standings"
        params = {'league': LEAGUE_ID, 'season': season}
        data = make_api_request(url, params)
        if data['response'] and data['response'][0]['league']['standings']:
            standings = data['response'][0]['league']['standings'][0]
            for team in standings:
                all_standings.append({
                    'season': season,
                    'team_id': team['team']['id'],
                    'team_name': team['team']['name'],
                    'position': team['rank'],
                    'points': team['points'],
                })
    df = pd.DataFrame(all_standings)
    df.to_csv('/tmp/standings_data.csv', index=False)
    return len(all_standings)

def merge_all_data(**context):
    df_fixtures = pd.read_csv('/tmp/fixtures_base.csv')
    df_standings = pd.read_csv('/tmp/standings_data.csv')

    # merge con standings home
    df_final = df_fixtures.merge(
        df_standings.rename(columns={'team_id': 'home_id', 'position': 'position_home', 'points': 'points_home'}),
        on=['home_id', 'season'], how='left'
    )
    # merge con standings away
    df_final = df_final.merge(
        df_standings.rename(columns={'team_id': 'away_id', 'position': 'position_away', 'points': 'points_away'}),
        on=['away_id', 'season'], how='left'
    )

    # features derivadas
    df_final['points_diff'] = df_final['points_home'] - df_final['points_away']
    df_final['position_diff'] = df_final['position_away'] - df_final['position_home']

    output_path = '/tmp/liga_profesional_dataset.csv'
    df_final.to_csv(output_path, index=False)

    return output_path

def cleanup_temp_files(**context):
    temp_files = ['/tmp/fixtures_base.csv','/tmp/standings_data.csv']
    for file in temp_files:
        try:
            if os.path.exists(file):
                os.remove(file)
                print(f"Archivo temporal eliminado: {file}")
        except Exception as e:
            print(f"Error eliminando {file}: {e}")

# Configuración del DAG
default_args = {
    'owner': 'grupo-18',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='liga_profesional_data_extraction',
    default_args=default_args,
    description='Extracción de datos de Liga Profesional Argentina desde API-FOOTBALL',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['football', 'liga-profesional', 'data-extraction']
)

task_extract_fixtures = PythonOperator(task_id='extract_fixtures', python_callable=extract_fixtures, dag=dag)
task_extract_standings = PythonOperator(task_id='extract_standings', python_callable=extract_standings_data, dag=dag)
task_merge_data = PythonOperator(task_id='merge_all_data', python_callable=merge_all_data, dag=dag)
task_cleanup = PythonOperator(task_id='cleanup_temp_files', python_callable=cleanup_temp_files, dag=dag)

# Dependencias
[task_extract_fixtures, task_extract_standings] >> task_merge_data
task_merge_data >> task_cleanup
