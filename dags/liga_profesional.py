# Archivo DAG de Airflow para procesar datos de la Liga Profesional Argentina
# Obtiene datos de equipos, posiciones, estadísticas y partidos desde una API de fútbol
# y los combina en un archivo CSV para análisis

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import os
import json
import time
import pandas as pd

# ID de la liga en la API (Liga Profesional Argentina)
LEAGUE_ID = 128
# Temporadas a procesar
SEASONS = [2021, 2022, 2023]
# IDs de equipos por temporada
TEAM_IDS = {
    2021: [434, 435, 436, 437, 438, 439, 441, 442, 444, 445, 446, 448, 449, 450, 451, 453, 455, 456, 457, 458, 459, 460, 463, 474, 1064, 1065],
    2022: [434, 435, 436, 437, 438, 439, 441, 442, 444, 445, 446, 448, 449, 450, 451, 452, 453, 455, 456, 457, 458, 459, 460, 463, 474, 1064, 1065, 2432],
    2023: [434, 435, 436, 437, 438, 439, 440, 441, 442, 445, 446, 448, 449, 450, 451, 452, 453, 455, 456, 457, 458, 459, 460, 474, 478, 1064, 1065, 2432],
}
# Directorio temporal para almacenar datos
DATA_DIR = "/tmp/liga_argentina"
# Clave de API para acceder a la API de fútbol
API_KEY = os.getenv("FOOTBALL_API_KEY")

# Argumentos por defecto para el DAG
default_args = {
    "owner": "grupo-18",
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# Función auxiliar para hacer peticiones GET a la API con autenticación
def _get(endpoint, hook):
    headers = {"x-apisports-key": API_KEY}
    return hook.run(endpoint, headers=headers).json()


# Función para obtener datos de equipos de todas las temporadas
def fetch_teams(**_):
    hook = HttpHook(http_conn_id="football_api", method="GET")
    all_teams = []
    os.makedirs(DATA_DIR, exist_ok=True)
    for season in SEASONS:
        data = _get(f"/teams?league={LEAGUE_ID}&season={season}", hook)
        for item in data.get("response", []):
            item["season"] = season
            all_teams.append(item)
        time.sleep(1)  # Pausa para evitar límites de rate de la API
    with open(os.path.join(DATA_DIR, "teams.json"), "w") as f:
        json.dump(all_teams, f)


# Función para obtener las posiciones finales de cada temporada
def fetch_standings(**_):
    hook = HttpHook(http_conn_id="football_api", method="GET")
    standings = []
    os.makedirs(DATA_DIR, exist_ok=True)
    for season in SEASONS:
        data = _get(f"/standings?league={LEAGUE_ID}&season={season}", hook)
        table = data.get("response", [])
        if table:
            rows = table[0]["league"]["standings"][0]
            for row in rows:
                row["season"] = season
                standings.append(row)
        time.sleep(1)  # Pausa para evitar límites de rate
    with open(os.path.join(DATA_DIR, "standings.json"), "w") as f:
        json.dump(standings, f)


# Función para obtener estadísticas detalladas de cada equipo por temporada
def fetch_team_statistics(**_):
    hook = HttpHook(http_conn_id="football_api", method="GET")
    stats = []
    os.makedirs(DATA_DIR, exist_ok=True)
    for season in SEASONS:
        for team in TEAM_IDS[season]:
            data = _get(f"/teams/statistics?league={LEAGUE_ID}&team={team}&season={season}", hook)
            data["season"] = season
            stats.append(data)
            time.sleep(6)  # Pausa para evitar límites de rate
    with open(os.path.join(DATA_DIR, "team_stats.json"), "w") as f:
        json.dump(stats, f)


# Función para obtener los partidos jugados (fixtures) de cada temporada
def fetch_fixtures(**_):
    hook = HttpHook(http_conn_id="football_api", method="GET")
    fixtures = []
    os.makedirs(DATA_DIR, exist_ok=True)
    for season in SEASONS:
        data = _get(
            f"/fixtures?league={LEAGUE_ID}&season={season}&status=ft", hook
        )
        for item in data.get("response", []):
            item["season"] = season
            fixtures.append(item)
        time.sleep(1)  # Pausa para evitar límites de rate
    with open(os.path.join(DATA_DIR, "fixtures.json"), "w") as f:
        json.dump(fixtures, f)



# Función para combinar y procesar los datos obtenidos, creando un DataFrame final
def merge_data(**_):
    # Cargar datos de posiciones y partidos
    with open(os.path.join(DATA_DIR, "standings.json")) as f:
        standings = json.load(f)
    with open(os.path.join(DATA_DIR, "fixtures.json")) as f:
        fixtures = json.load(f)

    # Procesar datos de posiciones en registros estructurados
    standings_records = [
        {
            "season": s.get("season"),
            "team_id": s.get("team", {}).get("id"),
            "rank": s.get("rank"),
            "points": s.get("points"),
            "goal_diff": s.get("goalsDiff"),
            "form": s.get("form"),
        }
        for s in standings
    ]

    # Procesar datos de partidos, calculando resultados y estadísticas
    fixtures_records = []
    for fx in fixtures:
        home_goals = (fx.get("goals", {}) or {}).get("home")
        away_goals = (fx.get("goals", {}) or {}).get("away")
        if home_goals is None or away_goals is None:
            outcome = None
        elif home_goals > away_goals:
            outcome = "H"  # Victoria local
        elif home_goals < away_goals:
            outcome = "A"  # Victoria visitante
        else:
            outcome = "D"  # Empate
        total_goals = home_goals + away_goals if home_goals is not None and away_goals is not None else None

        fixtures_records.append(
            {
                "fixture_id": (fx.get("fixture", {}) or {}).get("id"),
                "season": (fx.get("league", {}) or {}).get("season"),
                "match_date": (fx.get("fixture", {}) or {}).get("date"),
                "round": (fx.get("league", {}) or {}).get("round"),
                "home_team_id": ((fx.get("teams", {}) or {}).get("home", {}) or {}).get("id"),
                "home_team_name": ((fx.get("teams", {}) or {}).get("home", {}) or {}).get("name"),
                "away_team_id": ((fx.get("teams", {}) or {}).get("away", {}) or {}).get("id"),
                "away_team_name": ((fx.get("teams", {}) or {}).get("away", {}) or {}).get("name"),
                "home_goals": home_goals,
                "away_goals": away_goals,
                "outcome": outcome,
                "total_goals": total_goals,
            }
        )

    # Crear DataFrames de pandas
    standings_df = pd.DataFrame(standings_records)
    fixtures_df = pd.DataFrame(fixtures_records)

    # Ajustar tipos de datos para merges
    fixtures_df["season"] = fixtures_df["season"].astype(int)
    fixtures_df["home_team_id"] = fixtures_df["home_team_id"].astype(str)
    fixtures_df["away_team_id"] = fixtures_df["away_team_id"].astype(str)
    standings_df["season"] = standings_df["season"].astype(int)
    standings_df["team_id"] = standings_df["team_id"].astype(str)

    # Calcular métricas para equipos locales
    home_metrics = (
        fixtures_df.groupby(["season", "home_team_id"])
        .agg(
            home_avg_goals_for=("home_goals", "mean"),
            home_avg_goals_against=("away_goals", "mean"),
            home_clean_sheets=("away_goals", lambda x: (x == 0).sum()),
        )
        .reset_index()
    )

    # Calcular métricas para equipos visitantes
    away_metrics = (
        fixtures_df.groupby(["season", "away_team_id"])
        .agg(
            away_avg_goals_for=("away_goals", "mean"),
            away_avg_goals_against=("home_goals", "mean"),
            away_clean_sheets=("home_goals", lambda x: (x == 0).sum()),
        )
        .reset_index()
    )

    # Preparar estadísticas para equipos locales
    home_stats = (
        standings_df.rename(
            columns={
                "team_id": "home_team_id",
                "rank": "home_rank",
                "points": "home_points",
                "goal_diff": "home_goals_diff",
                "form": "home_form",
            }
        ).merge(home_metrics, on=["season", "home_team_id"], how="left")
    )

    # Preparar estadísticas para equipos visitantes
    away_stats = (
        standings_df.rename(
            columns={
                "team_id": "away_team_id",
                "rank": "away_rank",
                "points": "away_points",
                "goal_diff": "away_goals_diff",
                "form": "away_form",
            }
        ).merge(away_metrics, on=["season", "away_team_id"], how="left")
    )

    # Combinar todos los datos en un DataFrame final
    df = (
        fixtures_df.merge(home_stats, on=["season", "home_team_id"], how="left")
        .merge(away_stats, on=["season", "away_team_id"], how="left")
    )

    # Seleccionar y ordenar columnas finales
    df = df[
        [
            "fixture_id",
            "match_date",
            "season",
            "round",
            "home_team_id",
            "home_team_name",
            "away_team_id",
            "away_team_name",
            "home_goals",
            "away_goals",
            "outcome",
            "total_goals",
            "home_rank",
            "away_rank",
            "home_points",
            "away_points",
            "home_goals_diff",
            "away_goals_diff",
            "home_form",
            "away_form",
            "home_avg_goals_for",
            "home_avg_goals_against",
            "away_avg_goals_for",
            "away_avg_goals_against",
            "home_clean_sheets",
            "away_clean_sheets",
        ]
    ].sort_values(["season", "match_date"])

    # Guardar el DataFrame final como CSV
    output_path = os.path.join(DATA_DIR, "liga_argentina.csv")
    df.to_csv(output_path, index=False)
    print(f"Datos combinados guardados en {output_path}")

# Definición del DAG de Airflow
with DAG(
    dag_id="datos_liga_profesional",
    description="Obtiene y prepara datos de la Liga Profesional Argentina",
    default_args=default_args,
    schedule=None,  # No programado, se ejecuta manualmente
    catchup=False,  # No ejecutar ejecuciones pasadas
    tags=["football", "argentina", "etl"],
) as dag:

    # Tarea para verificar que la API esté disponible
    check_api = HttpOperator(
        task_id="check_api",
        http_conn_id="football_api",
        endpoint="/status",
        headers={"x-apisports-key": API_KEY},
        method="GET",
    )

    # Tareas para obtener datos (comentada la de equipos para ahorrar requests)
    # teams_task = PythonOperator(task_id="fetch_teams", python_callable=fetch_teams) Ahorro request
    standings_task = PythonOperator(task_id="fetch_standings", python_callable=fetch_standings)
    #stats_task = PythonOperator(task_id="fetch_team_statistics", python_callable=fetch_team_statistics)
    fixtures_task = PythonOperator(task_id="fetch_fixtures", python_callable=fetch_fixtures)
    merge_task = PythonOperator(task_id="merge_data", python_callable=merge_data)

    # Verificar API primero, luego obtener datos en paralelo, y finalmente combinar
    check_api >> [standings_task, fixtures_task] >> merge_task