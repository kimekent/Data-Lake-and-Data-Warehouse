from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from get_weather_apache import get_weather
from send_to_dwh_apache import load_dwh

#set default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 6),
    'email': ['lpwymann@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


dag = DAG(
    'fetch_weather_data',
    default_args=default_args,
    description= "fetches weather data and sends it to data lake then to data warehouse",
    schedule_interval= "@daily", #run once a day at midnight
    catchup=False #no need to go back in time since making forecasts
)

extract_transform = PythonOperator(
    task_id= 'connect_API_fetch_send',
    python_callable=get_weather,
    dag=dag

)

load = PythonOperator(
    task_id= 'create_DWH_table_load_data',
    python_callable=load_dwh,
    dag=dag

)

extract_transform>>load
