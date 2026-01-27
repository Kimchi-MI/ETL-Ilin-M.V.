from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def process_json_file():
    with open('/opt/airflow/dags/data/pets-data.json', 'r') as f:
        data = json.load(f)
    
    linear_data = []
    for pet in data['pets']:
        record = {
            'name': pet['name'],
            'species': pet['species'],
            'fav_foods': ', '.join(pet.get('favFoods', [])) if pet.get('favFoods') else None,
            'birth_year': pet['birthYear'],
            'photo': pet.get('photo', '')
        }
        linear_data.append(record)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pets_linear (
            id SERIAL PRIMARY KEY,
            pet_name VARCHAR(100),
            species VARCHAR(50),
            fav_foods TEXT,
            birth_year INTEGER,
            photo_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("DELETE FROM pets_linear")
    
    for record in linear_data:
        cursor.execute("""
            INSERT INTO pets_linear (pet_name, species, fav_foods, birth_year, photo_url)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            record['name'],
            record['species'],
            record['fav_foods'],
            record['birth_year'],
            record['photo']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Обработано {len(linear_data)} записей")

with DAG(
    'json_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    process_json = PythonOperator(
        task_id='process_json',
        python_callable=process_json_file
    )
    
    process_json
