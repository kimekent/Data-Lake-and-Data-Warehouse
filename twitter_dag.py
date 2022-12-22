from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from get_tweets_apache import run_get_tweets

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 3, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='Getting_Tweets',
    schedule_interval=timedelta(days=1),
)


run_etl = PythonOperator(
    task_id='Getting_Tweets',
    python_callable=run_get_tweets,
    dag=dag,
)

run_etl