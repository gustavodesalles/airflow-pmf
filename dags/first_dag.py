from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='first_dag_v4',
    default_args=default_args,
    description='A simple first DAG',
    start_date=datetime(2023, 10, 1),
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
    task1 = BashOperator(task_id='first_task', bash_command='echo "Hello World!"', dag=dag) # Placeholder for tasks, if any
    task2 = BashOperator(task_id='second_task', bash_command='echo "It is good to be here!"', dag=dag)
    task3 = BashOperator(task_id='third_task', bash_command='echo "I hope this works!"', dag=dag)

    # task1.set_downstream(task2)
    # task1.set_downstream(task3)
    # task1 >> task2
    # task1 >> task3
    task1 >> [task2, task3]