from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1)
}

dag = DAG(
    'count_tables_dag',
    default_args=default_args,
    schedule_interval='@daily'
)

def count_tables():
    hook = PostgresHook(postgres_conn_id='postgres')
    tables = hook.get_tables()
    table_count = len(tables)
    print(f"테이블 수: {table_count}")

count_task = PythonOperator(
    task_id='count_tables',
    python_callable=count_tables,
    dag=dag
)

count_task
