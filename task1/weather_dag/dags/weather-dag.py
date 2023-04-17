import requests
import time
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

def _extract_data(execution_date):
    url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
    params = {'lat': 49.841952, 
              'lon': 24.0315921, 
              'dt': int(time.mktime(execution_date.timetuple())),
              'exclude': "minutely,hourly,daily,alerts",
              'units': "metric",
              "APPID": Variable.get("weather_appid")
              }
    response = requests.get(url, params=params)
    return response.json()

def _process_weather(ti):
    info = ti.xcom_pull(task_ids="extract_data", key='return_value')["data"][0]
    timestamp = info["dt"]
    temp = info["temp"]
    humidity = info["humidity"]
    clouds = info["clouds"]
    wind_speed = info["wind_speed"]
    return timestamp, temp, humidity, clouds, wind_speed

with DAG(dag_id="weather", 
         schedule_interval="@daily", 
         start_date=days_ago(5),
         catchup=True
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