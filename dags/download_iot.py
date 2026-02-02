import logging
import pendulum
import kagglehub

from kagglehub import KaggleDatasetAdapter
from kaggle.api.kaggle_api_extended import KaggleApi

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import os
import json

# Конфигурация DAG
OWNER = "a.sorokin"
DAG_ID = "download_iot"


SHORT_DESCRIPTION = "Скачивание данных c Iot-устройств"

LONG_DESCRIPTION = """В данном DAG расматривается простой ETL процесс в виде скачивания, очистки 
                    и раскалыдвания данных внутрь Postgresql, a затем производится визуализация через Superset"""

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(year=2026, month=1, day=30),
    "retries": 3,
    "depends_on_past": False
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


    def download_data(**context):
        kaggle_json_path = '/home/airflow/.config/kaggle/kaggle.json'        
        
        if not os.path.exists(kaggle_json_path):
            raise FileNotFoundError("Файл kaggle.json не найден")

        with open(kaggle_json_path, 'r') as f:
            kaggle_json = json.load(f)
        
        os.environ['KAGGLE_USERNAME'] = kaggle_json['username']
        os.environ['KAGGLE_KEY'] = kaggle_json['key']

        api = KaggleApi()
        api.authenticate()

        api.dataset_download_files('atulanandjha/temperature-readings-iot-devices', path='/opt/airflow/data', unzip=True)
        logging.info("Данные успешно загружены с Kaggle!")

    download_data = PythonOperator(
        task_id = "download_data",
        python_callable = download_data,
    )

    end = EmptyOperator(
        task_id = "end"
    )

start >> download_data >> end
