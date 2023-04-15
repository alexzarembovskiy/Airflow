import requests
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

def _extract_data():
    url = "https://api.openweathermap.org/data/3.0/onecall"
    params = {'lat': 49.841952, 
              'lon': 24.0315921, 
              'exclude': "minutely,hourly,daily,alerts",
              'units': "metric",
              "APPID": Variable.get("weather_appid")
              }
    response = requests.get(url, params=params)
    return response.json()

def _process_weather(ti):
    info = ti.xcom_pull(task_ids="extract_data", key='return_value')
    timestamp = info["current"]["dt"]
    temp = info["current"]["temp"]
    humidity = info["current"]["humidity"]
    clouds = info["current"]["clouds"]
    wind_speed = info["current"]["wind_speed"]
    return timestamp, temp, humidity, clouds, wind_speed

with DAG(dag_id="weather", 
         schedule_interval="@daily", 
         start_date=days_ago(2)
         ) as dag:
    
    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="measurements_db",
        sql="""
            CREATE TABLE IF NOT EXISTS weather (
            execution_time TIMESTAMP NOT NULL,
            temperature FLOAT NOT NULL,
            humidity FLOAT NOT NULL,
            cloudiness FLOAT NOT NULL,
            wind_speed FLOAT NOT NULL
            );
          """,
    )

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=_process_weather,
    )

    inject_data = PostgresOperator(
        task_id="inject_data",
        postgres_conn_id="measurements_db",
        sql="""
        INSERT INTO weather (execution_time, temperature, humidity, cloudiness, wind_speed) VALUES 
        (to_timestamp({{ti.xcom_pull(task_ids='process_data')[0]}}), 
        {{ti.xcom_pull(task_ids='process_data')[1]}},
        {{ti.xcom_pull(task_ids='process_data')[2]}},
        {{ti.xcom_pull(task_ids='process_data')[3]}},
        {{ti.xcom_pull(task_ids='process_data')[4]}});
        """,
    )
    
    create_postgres_table >> extract_data >> process_data >> inject_data