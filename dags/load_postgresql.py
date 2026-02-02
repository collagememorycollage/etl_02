import logging
import pendulum
import kagglehub

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.sensors.python import PythonSensor

from airflow.sensors.external_task import ExternalTaskSensor
import os
from pathlib import Path
import pandas as pd
# Конфигурация DAG
OWNER = "a.sorokin"
DAG_ID = "load_postrgesql"

SHORT_DESCRIPTION = "Скачивание данных c Iot-устройств"

LONG_DESCRIPTION = """В данном DAG расматривается простой ETL процесс в виде скачивания, очистки 
                    и раскалыдвания данных внутрь Postgresql, a затем производится визуализация через Superset"""

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2026, month=1, day=30),
    "retries": 3,
    "depends_on_past": True
}

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

start >> end

