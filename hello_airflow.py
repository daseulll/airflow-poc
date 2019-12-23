from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

dag = DAG(
    'hello-airflow', description='Hello airflow DAG',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2019, 12, 20),
    catchup=False 
    )

def print_hello():
    return 'Hello Airflow'

python_task = PythonOperator(
    task_id='python_operator',
    python_callable=print_hello,
    dag=dag
)

bash_task = BashOperator(
    task_id='bash_operator',
    bash_command='date',
    dag=dag
)

bash_task.set_downstream(python_task)