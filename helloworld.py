from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'helloworld',
    default_args=default_args,
    description='echo "hello, world!"',
    schedule_interval=timedelta(days=1),
)

t1 = BashOperator(
    task_id='echo_hello_world',
    bash_command='echo "Hello, World!"',
    dag=dag,
)
