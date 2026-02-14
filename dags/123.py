from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# Определяем SQL запросы прямо здесь или выносим в отдельные файлы
create_table_sql = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    signup_date DATE
);
"""

insert_data_sql = """
INSERT INTO users (name, signup_date) VALUES 
('Alice', '2023-01-01'),
('Bob', '2023-01-02'),
('Charlie', '2023-01-03');
"""

with DAG(
    dag_id="simple_postgres_etl",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    # Шаг 1: Создаем структуру
    create_task = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postrges-db", # Ссылаемся на Connection, который создали в UI
sql=create_table_sql
    )

    # Шаг 2: Чистим старые данные (для идемпотентности)
    clean_task = SQLExecuteQueryOperator(
        task_id="clean_table",
        conn_id="postrges-db",
        sql="TRUNCATE TABLE users;"
    )

    # Шаг 3: Наливаем данные
    fill_task = SQLExecuteQueryOperator(
        task_id="fill_table",
        conn_id="postrges-db",
        sql=insert_data_sql
    )

    # Задаем порядок выполнения
    create_task >> clean_task >> fill_task
