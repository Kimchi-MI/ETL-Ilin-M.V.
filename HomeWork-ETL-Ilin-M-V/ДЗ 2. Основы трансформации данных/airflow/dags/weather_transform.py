from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import numpy as np

default_args = {
    'owner': 'student_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform_weather_data():
    df = pd.read_csv('/opt/airflow/data/dataset.csv')
    
    print("Колонки в датасете:", df.columns.tolist())
    
    df = df[df['out/in'] == 'in'].copy()
    print(f"После фильтрации 'in': {len(df)} записей")
    
    df['noted_date'] = pd.to_datetime(df['noted_date'], errors='coerce')
    df['noted_date'] = df['noted_date'].dt.strftime('%Y-%m-%d')
    df['noted_date'] = pd.to_datetime(df['noted_date']).dt.date

    lower_bound = df['temp'].quantile(0.05) 
    upper_bound = df['temp'].quantile(0.95)  #
    df['temp'] = df['temp'].clip(lower=lower_bound, upper=upper_bound)  
    
    daily_avg = df.groupby('noted_date')['temp'].mean().reset_index()  
    hottest_days = daily_avg.nlargest(5, 'temp')  
    coldest_days = daily_avg.nsmallest(5, 'temp')  

    hottest_days.to_csv('/opt/airflow/data/hottest_days.csv', index=False)
    coldest_days.to_csv('/opt/airflow/data/coldest_days.csv', index=False)
    df.to_csv('/opt/airflow/data/processed_data.csv', index=False)
    
    print("Трансформация завершена успешно!")
    print(f"5 самых жарких дней:\n{hottest_days}")
    print(f"\n5 самых холодных дней:\n{coldest_days}")
    print(f"\nСохранено {len(df)} записей")

with DAG(
    'weather_data_transformation',
    default_args=default_args,
    description='Трансформация данных погоды',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    start_task = DummyOperator(task_id='start')
    transform_task = PythonOperator(task_id='transform_weather_data', python_callable=transform_weather_data)
    end_task = DummyOperator(task_id='end')
    
    start_task >> transform_task >> end_task
