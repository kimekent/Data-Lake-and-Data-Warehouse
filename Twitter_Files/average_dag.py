from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from average import run_average

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'average_dag',
    default_args=default_args,
    description='Creating Tableau Table',
    schedule_interval=timedelta(days=1),
)


run_etl = PythonOperator(
    task_id='tableau_table',
    python_callable=run_average,
    dag=dag,
)

run_average