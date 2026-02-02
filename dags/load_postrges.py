import logging
import pendulum
import kagglehub


from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor

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
    db_user = "postgres"
    db_password = "postgres"
    db_host = "etl_02-postgres_storage_db-1"
    db_port = "5432"
    db_name = "postgres"

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    try:
        # Пробуем выполнить простой SQL-запрос
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("Подключение успешно:", result.scalar())
            return True
    except Exception as e:
        print("Ошибка подключения:", e)
        return False


def load_csv_to_DB():
    db_user = "postgres"
    db_password = "postgres"
    db_host = "etl_02-postgres_storage_db-1"
    db_port = "5432"
    db_name = "postgres"

    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    csv_path = "/opt/airflow/data/IOT-temp_clean.csv"   # путь к CSV
    table_name = "iot_data"

    df = pd.read_csv(csv_path)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Данные успешно загружены в таблицу {table_name}")


with DAG(
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    default_args=args,
    schedule_interval="0 10 * * *",
    tags=["download", "postgres", "convert"],
    concurrency=1
) as dag:

    dag.doc_md=LONG_DESCRIPTION

    start = EmptyOperator(
        task_id = "start"
    )

    end = EmptyOperator(
        task_id = "end"
    )

    check_parse = ExternalTaskSensor(
        task_id="check_parse",
        external_dag_id="parse_iot",
        mode="reschedule",
        poke_interval=60,
        timeout=3600
    )

    check_connect_to_DB = PythonSensor(
        task_id="check_connect_to_DB",
        python_callable=check_connect_to_DB
    )



    load_csv_to_DB = PythonOperator(
        task_id="load_csv_to_DB",
        python_callable=load_csv_to_DB
    )

start >>  check_parse >> check_connect_to_DB >> load_csv_to_DB >> end

