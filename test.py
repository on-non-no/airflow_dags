from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def count_tables():
    # Postgres 연결 설정
    hook = PostgresHook(postgres_conn_id='postgres')
    
    # 테이블 수를 쿼리하여 가져옴
    query = "SELECT COUNT(*) FROM test"
    result = hook.get_first(query)
    table_count = result[0]
    
    # 테이블 수를 출력
    print(f"테이블 수: {table_count}")

# DAG 설정
default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 2, 13),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('count_tables_dag', default_args=default_args, schedule_interval='@daily')

# PythonOperator를 사용하여 count_tables 함수 실행
count_tables_task = PythonOperator(
    task_id='count_tables_task',
    python_callable=count_tables,
    dag=dag
)

count_tables_task
