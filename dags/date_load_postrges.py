import logging
import pendulum
import kagglehub
from pathlib import Path
from kagglehub import KaggleDatasetAdapter
from kaggle.api.kaggle_api_extended import KaggleApi
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
DAG_ID = "date_load_postgres"


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


def load_date_csv_to_postgres():
    # Тут можно было бы передать текущую дату через контекст (**context)
    date = "2018-12-08"

    hook = PostgresHook(postgres_conn_id="postgres-db")

    csv_path = f"/opt/airflow/data/IOT-temp_clean_{date}.csv"   # путь к CSV
    table_name = "date_raw_data"

    df = pd.read_csv(csv_path)

    df.to_sql(table_name, hook.get_sqlalchemy_engine(), if_exists='append', index=False)

    print(f"Данные успешно загружены в таблицу {table_name}")


def parse_data():
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

def check_parse_data(**context):
    out_put_path = context["output_path"]
    path = Path(out_put_path)

    if path.exists():
        return True
    else:
        logging.warn("Файл не найден")
        return False

def get_date():
    # Тут можно было бы передать текущую дату через контекст (**context)
    date = "2018-12-08"

    csv_path = "/opt/airflow/data/IOT-temp_clean.csv"   # путь к CSV
    df = pd.read_csv(csv_path)

    df["noted_date"] = pd.to_datetime(df["noted_date"])

    df_date = df[df["noted_date"] == "2018-12-08"]

    with open(f"/opt/airflow/data/IOT-temp_clean_{date}.csv", 'w') as f:
        f.write(",".join(df_date.columns) + "\n")

        for _, row in df_date.iterrows():
            f.write(",".join(map(str, row.values)) + "\n")

with DAG(
    dag_id=DAG_ID,
    description=SHORT_DESCRIPTION,
    default_args=args,
    schedule_interval="0 10 * * *",
    tags=["download", "postgres", "convert"],
    concurrency=1,
    catchup=False
) as dag:
   

    start = EmptyOperator(
        task_id="start"
    )
   
    end = EmptyOperator(
        task_id="end")

    check_connect_to_DB = PythonSensor(
        task_id="check_connect_to_DB",
        python_callable=check_connect_to_DB
    )

    download_data = PythonOperator(
        task_id = "download_data",
        python_callable = download_data,
    )


    check_file = PythonSensor(
        task_id="check_file",
        python_callable=check_file,
        op_kwargs={"output_path": "/opt/airflow/data/IOT-temp.csv"}
    )


    parse_data = PythonOperator(
        task_id="parse_data",
        python_callable=parse_data,
        op_kwargs={"output_path": "/opt/airflow/data/IOT-temp.csv"}
    )

    check_parse_data = PythonSensor(
        task_id="check_parse_data",
        python_callable=check_parse_data,
        op_kwargs={"output_path": "/opt/airflow/data/IOT-temp_clean.csv"}
    )

    load_date_csv_to_postgres = PythonOperator(
        task_id="load_date_csv_to_postgres",
        python_callable=load_date_csv_to_postgres
    )

    get_date = PythonOperator(
        task_id="get_date",
        python_callable=get_date
    )

start >> download_data >> check_file >> parse_data >> check_parse_data >> get_date >> check_connect_to_DB >> load_date_csv_to_postgres >> end
