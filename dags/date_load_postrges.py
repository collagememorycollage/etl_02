import logging
import pendulum
import kagglehub


from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

# Конфигурация DAG
OWNER = "a.sorokin"
DAG_ID = "load_postgres"


SHORT_DESCRIPTION = "Скачивание данных c Iot-устройств"

LONG_DESCRIPTION = """В данном DAG расматривается простой ETL процесс в виде скачивания, очистки 
                    и раскалыдвания данных внутрь Postgresql, a затем производится визуализация через Superset"""

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2026, month=1, day=30),
    "retries": 3,
    "depends_on_past": False
}


def check_connect_to_DB():
    hook = PostgresHook(postgres_conn_id="postgres-db")

    status, msg = hook.test_connection()

    if not status:
        print(f"Подключение не удалось {msg}")
        return False
    print(f"Подключение удалось {msg}")
    return True

def load_csv_to_postgres():
    hook = PostgresHook(postgres_conn_id="postgres-db")

    csv_path = "/opt/airflow/data/IOT-temp_clean.csv"   # путь к CSV
    table_name = "raw_data"

    df = pd.read_csv(csv_path)

    df.to_sql(table_name, hook.get_sqlalchemy_engine(), if_exists='replace', index=False)

    print(f"Данные успешно загружены в таблицу {table_name}")


with DAG(
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    default_args=args,
    schedule_interval="0 10 * * *",
    tags=["download", "postgres", "convert"],
    concurrency=1
) as dag:
    
