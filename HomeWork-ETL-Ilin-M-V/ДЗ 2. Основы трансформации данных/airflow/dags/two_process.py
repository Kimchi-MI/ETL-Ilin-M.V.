from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def process1_full():
    df = pd.read_csv('/opt/airflow/data/dataset.csv')
    df = df.rename(columns={'temp': 'temperature', 'out/in': 'out_in'})
    df = df[df['out_in'].str.lower() == 'in']
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna(subset=['noted_date'])
    df['noted_date'] = df['noted_date'].dt.strftime('%Y-%m-%d')
    lower = df['temperature'].quantile(0.05)
    upper = df['temperature'].quantile(0.95)
    df = df[(df['temperature'] >= lower) & (df['temperature'] <= upper)]
    
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/weather_data')
    df.to_sql('filtered_temperature_full', engine, if_exists='replace', index=False)

def process2_inc():
    df = pd.read_csv('/opt/airflow/data/dataset.csv')
    df = df.rename(columns={'temp': 'temperature', 'out/in': 'out_in'})
    df = df[df['out_in'].str.lower() == 'in']
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna(subset=['noted_date'])
    df['noted_date'] = df['noted_date'].dt.strftime('%Y-%m-%d')
    lower = df['temperature'].quantile(0.05)
    upper = df['temperature'].quantile(0.95)
    df = df[(df['temperature'] >= lower) & (df['temperature'] <= upper)]
    
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/weather_data')
    
    df_simple = df[['noted_date', 'temperature', 'out_in']]
    df_simple.to_sql('filtered_temperature_inc', engine, if_exists='append', index=False)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'two_process_simple',
    default_args=default_args,
    description='Два процесса загрузки',
    schedule_interval='@once',
    catchup=False,
) as dag:
    
    task1 = PythonOperator(
        task_id='process1_full',
        python_callable=process1_full,
    )
    
    task2 = PythonOperator(
        task_id='process2_inc',
        python_callable=process2_inc,
    )
    
    task1 >> task2