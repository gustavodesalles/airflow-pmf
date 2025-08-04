from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    name = ti.xcom_pull(task_ids='get_name', key='name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f'{name} {last_name} is {age} years old and I am getting real tired of this.')

def get_name(ti):
    ti.xcom_push(key='name', value='Timmy')
    ti.xcom_push(key='last_name', value='Turner')

def get_age(ti):
    ti.xcom_push(key='age', value=23)

with DAG(
    default_args=default_args,
    dag_id='dag_with_python_operator_v6',
    description='A simple DAG with PythonOperator',
    start_date=datetime(2023, 10, 1),
    schedule=timedelta(days=1),
    catchup=False,
) as dag:
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=greet,
        # op_kwargs={'age': 30},
        dag=dag
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name,
        dag=dag
    )

    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age,
        dag=dag
    )

    [task2, task3] >> task1