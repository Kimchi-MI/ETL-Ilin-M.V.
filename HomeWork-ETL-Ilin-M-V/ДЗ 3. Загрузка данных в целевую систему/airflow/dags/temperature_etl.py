from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def transform_and_save_to_sql():
    df = pd.read_csv('/opt/airflow/data/dataset.csv')
    
    df = df.rename(columns={'temp': 'temperature', 'out/in': 'out_in'})
    df = df[df['out_in'].str.lower() == 'in']
    
    df['noted_date'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
    df = df.dropna(subset=['noted_date'])
    df['noted_date'] = df['noted_date'].dt.strftime('%Y-%m-%d')
    
    lower = df['temperature'].quantile(0.05)
    upper = df['temperature'].quantile(0.95)
    df = df[(df['temperature'] >= lower) & (df['temperature'] <= upper)]
    
    daily_avg = df.groupby('noted_date')['temperature'].mean().reset_index()
    hottest = daily_avg.nlargest(5, 'temperature')
    coldest = daily_avg.nsmallest(5, 'temperature')
    
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/weather_data')
    
    df.to_sql('filtered_temperature', engine, if_exists='replace', index=False)
    hottest.to_sql('hottest_days', engine, if_exists='replace', index=False)
    coldest.to_sql('coldest_days', engine, if_exists='replace', index=False)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'temperature_etl_to_postgres',
    default_args=default_args,
    description='ETL для температурных данных',
    schedule_interval='@once',
    catchup=False,
) as dag:
    
    transform_task = PythonOperator(
        task_id='transform_and_save_to_sql',
        python_callable=transform_and_save_to_sql,
    )