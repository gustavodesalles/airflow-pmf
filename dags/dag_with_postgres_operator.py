from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_postgres_operator_v1',
    default_args=default_args,
    start_date=datetime(2025, 8, 4),
    schedule=timedelta(days=1)
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id='postgres_localhost',
        sql="""
            drop table if exists dag_runs;
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )
    insert_text = f"insert into dag_runs(dt, dag_id) values ('{datetime.now().strftime("%Y-%m-%d")}','A')"

    task2 = SQLExecuteQueryOperator(
        task_id='insert_data',
        conn_id='postgres_localhost',
        sql=insert_text
    )

    task3 = SQLExecuteQueryOperator(
        task_id='insert_from_file',
        conn_id='postgres_localhost',
        sql='/sql/example.sql'
    )
    task1 >> [task2, task3]