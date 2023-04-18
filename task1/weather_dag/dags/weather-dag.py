import requests
import time
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

cities = {
        'Lviv': {'lat': 49.841952, 'lon': 24.0315921},
        'Kyiv':  {'lat': 50.4500336, 'lon': 30.5241361},
        'Kharkiv': {'lat': 49.9923181, 'lon': 36.2310146},
        'Odesa': {'lat': 46.4843023, 'lon': 30.7322878},
        'Zhmerynka': {'lat': 49.0354593, 'lon': 28.1147317}
    }

def _extract_data(execution_date):
    resulting_list = []
    url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
    for city in cities:

        params = {'lat': cities[city]['lat'], 
                'lon': cities[city]['lon'], 
                'dt': int(time.mktime(execution_date.timetuple())),
                'exclude': "minutely,hourly,daily,alerts",
                'units': "metric",
                "APPID": Variable.get("weather_appid")
                }
        
        response_data = requests.get(url, params=params).json()['data'][0]

        resulting_list.append(
            {
            'timestamp': response_data["dt"],
            'temp': response_data["temp"],
            'humidity': response_data["humidity"],
            'clouds': response_data["clouds"],
            'wind_speed': response_data['wind_speed'],
            'city_name': city
            }
        )
    return resulting_list

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
            wind_speed FLOAT NOT NULL,
            city VARCHAR NOT NULL
            );
          """,
    )

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
    )

    tasks = []
    for i in range(len(cities)):
        inject_data = PostgresOperator(
            #info = task_instance.xcom_pull(task_ids='extract_data')[i],
            task_id=f"inject_data_{i}",
            postgres_conn_id="measurements_db",
            sql=f"""
            INSERT INTO weather (execution_time, temperature, humidity, cloudiness, wind_speed, city) VALUES 
            (to_timestamp({{{{ti.xcom_pull(task_ids='extract_data')[{i}]['timestamp']}}}}), 
            {{{{ti.xcom_pull(task_ids='extract_data')[{i}]['temp']}}}},
            {{{{ti.xcom_pull(task_ids='extract_data')[{i}]['humidity']}}}},
            {{{{ti.xcom_pull(task_ids='extract_data')[{i}]['clouds']}}}},
            {{{{ti.xcom_pull(task_ids='extract_data')[{i}]['wind_speed']}}}},
            '{{{{ti.xcom_pull(task_ids='extract_data')[{i}]['city_name']}}}}'
            );
            """,
        )
        tasks.append(inject_data)
    
    create_postgres_table >> extract_data >> tasks