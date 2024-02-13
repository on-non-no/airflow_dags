from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from airflow.sensors.sql import SqlSensor

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    description='echo "hello, world!"',
    schedule_interval=timedelta(days=1),
)

sql_sensor_task = SqlSensor(
    task_id='sql_sensor_task',
    conn_id='postgres',  # 데이터베이스 연결 ID
    sql='SELECT COUNT(*) FROM test;',
    mode='poke',
    timeout=600,
    poke_interval=60,
    dag=dag
)

your_next_task = BashOperator(
    task_id='echo_hello_world',
    bash_command='echo "Hello, World!"',
    dag=dag,
)

sql_sensor_task >> your_next_task
