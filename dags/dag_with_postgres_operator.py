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
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )
    task1