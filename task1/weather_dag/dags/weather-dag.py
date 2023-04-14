from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(dag_id="weather", 
         schedule_interval="@daily", 
         start_date=days_ago(2)
         ) as dag:
    
    create_postgres_table = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="measurements_db",
        sql="""
            CREATE TABLE IF NOT EXISTS measurements (
            execution_time TIMESTAMP NOT NULL,
            temperature FLOAT
            );
          """,
    )

    extract_date = SimpleHttpOperator(
        task_id="extract_date",
        http_conn_id="weather_api_con",
        endpoint="data/2.5/weather",
        data={"appid": Variable.get("weather_appid"), "q": "Lviv"},
        method="GET",
        log_response=True
    )
    
    create_postgres_table >> extract_date