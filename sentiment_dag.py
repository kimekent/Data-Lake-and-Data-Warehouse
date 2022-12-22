from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from sentiment_airflow import run_sentiment

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'sentiment_dag',
    default_args=default_args,
    description='Sentiment_Tweets',
    schedule_interval=timedelta(days=1),
)

dag = DAG(
    'sentiment_dag',
    default_args=default_args,
    description='Sentiment_Tweets_Code'
)

run_sentiment_dag = PythonOperator(
    task_id='Sentiment_Tweets',
    python_callable=run_sentiment,
    dag=dag,
)

run_sentiment_dag