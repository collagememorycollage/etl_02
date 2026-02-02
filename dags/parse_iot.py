import logging
import pendulum
import kagglehub

from kagglehub import KaggleDatasetAdapter
from kaggle.api.kaggle_api_extended import KaggleApi

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
DAG_ID = "parse_iot"

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

    def check_file(**context):
        out_put_path = context['output_path']
        path = Path(out_put_path)
        print(context)
         
        if path.exists():
            return True
        else:
            logging.warn("Файл не найден")
            return False
        #data_path = "/opt/airflow/data"
        #data_file = "IOT-temp.csv"

    
    def clean_data():
        df = pd.read_csv('/opt/airflow/data/IOT-temp.csv')
        #Удаляем полные дубликаты
        df = df.drop_duplicates()
    
        # Удаление пропусков
        df = df.dropna()
        
        # Приводим столбец id к нужному формату и нужной структуре
        def edit(data):
            return data[20:]
    
        df['id'] = df['id'].apply(edit)

        # Приводим столбец noted_date к нужному формату и нужной структуре
        def parse_date(date_str):
            date_str = date_str[:-6]
            return pd.to_datetime(date_str, format="%d-%m-%Y")
    
        df['noted_date'] = df['noted_date'].apply(parse_date) 
       

        with open('/opt/airflow/data/IOT-temp_clean.csv', 'w') as f:
            f.write(",".join(df.columns) + "\n")

            for _, row in df.iterrows():
                f.write(",".join(map(str, row.values)) + "\n") 
        
    
    def log_info():
        df = pd.read_csv('/opt/airflow/data/IOT-temp_clean.csv')

        logging.info("5 САМЫX ТЕПЛЫX ДНЕЙ")
        print(df.sort_values('temp', ascending=False).head(5))

        logging.info("5 САМЫX ХОЛОДНЫХ ДНЕЙ")
        print(df.sort_values('temp', ascending=True).head(5))

        

    def sort_in_out():
        logging.info("text")

    sort_in_out = PythonOperator(
        task_id="sort_in_out",
        python_callable=sort_in_out
    )


    log_info = PythonOperator(
        task_id="log_info",
        python_callable=log_info
    )

    start = EmptyOperator(
        task_id = "start"
    )


    check_download_complete = ExternalTaskSensor(
        task_id = "check_download_complete",
        external_dag_id = "download_iot",
        mode="reschedule",
        poke_interval=60,
        timeout=3600
    )


    check_file = PythonSensor(
        task_id="check_file",
        python_callable=check_file,
        op_kwargs={"output_path": "/opt/airflow/data/IOT-temp.csv"}
    )

    clean_data = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data
    )

    end = EmptyOperator(
        task_id = "end"
    )

start >> check_download_complete >> check_file >> clean_data 
clean_data >> [log_info, sort_in_out]
sort_in_out >> end



