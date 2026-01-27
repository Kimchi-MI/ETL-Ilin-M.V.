from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import xml.etree.ElementTree as ET

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

def process_xml_file():
    tree = ET.parse('/opt/airflow/dags/data/nutrition.xml')
    root = tree.getroot()
    
    linear_data = []
    for food in root.findall('food'):
        record = {
            'name': food.find('name').text if food.find('name') is not None else '',
            'manufacturer': food.find('mfr').text if food.find('mfr') is not None else '',
            'serving': food.find('serving').text if food.find('serving') is not None else '',
            'serving_unit': food.find('serving').get('units') if food.find('serving') is not None else '',
            'calories_total': food.find('calories').get('total') if food.find('calories') is not None else '0',
            'calories_fat': food.find('calories').get('fat') if food.find('calories') is not None else '0',
            'total_fat': food.find('total-fat').text if food.find('total-fat') is not None else '0',
            'saturated_fat': food.find('saturated-fat').text if food.find('saturated-fat') is not None else '0',
            'cholesterol': food.find('cholesterol').text if food.find('cholesterol') is not None else '0',
            'sodium': food.find('sodium').text if food.find('sodium') is not None else '0',
            'carbs': food.find('carb').text if food.find('carb') is not None else '0',
            'fiber': food.find('fiber').text if food.find('fiber') is not None else '0',
            'protein': food.find('protein').text if food.find('protein') is not None else '0',
            'vitamin_a': food.find('vitamins/a').text if food.find('vitamins/a') is not None else '0',
            'vitamin_c': food.find('vitamins/c').text if food.find('vitamins/c') is not None else '0',
            'calcium': food.find('minerals/ca').text if food.find('minerals/ca') is not None else '0',
            'iron': food.find('minerals/fe').text if food.find('minerals/fe') is not None else '0'
        }
        linear_data.append(record)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nutrition_linear (
            id SERIAL PRIMARY KEY,
            food_name VARCHAR(255),
            manufacturer VARCHAR(255),
            serving_size VARCHAR(50),
            serving_unit VARCHAR(50),
            calories_total INTEGER,
            calories_fat INTEGER,
            total_fat NUMERIC,
            saturated_fat NUMERIC,
            cholesterol NUMERIC,
            sodium NUMERIC,
            carbs NUMERIC,
            fiber NUMERIC,
            protein NUMERIC,
            vitamin_a INTEGER,
            vitamin_c INTEGER,
            calcium INTEGER,
            iron INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cursor.execute("DELETE FROM nutrition_linear")
    
    for record in linear_data:
        cursor.execute("""
            INSERT INTO nutrition_linear (
                food_name, manufacturer, serving_size, serving_unit,
                calories_total, calories_fat, total_fat, saturated_fat,
                cholesterol, sodium, carbs, fiber, protein,
                vitamin_a, vitamin_c, calcium, iron
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record['name'], record['manufacturer'], 
            record['serving'], record['serving_unit'],
            int(record['calories_total']), int(record['calories_fat']),
            float(record['total_fat']), float(record['saturated_fat']),
            float(record['cholesterol']), float(record['sodium']),
            float(record['carbs']), float(record['fiber']),
            float(record['protein']),
            int(record['vitamin_a']), int(record['vitamin_c']),
            int(record['calcium']), int(record['iron'])
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Обработано {len(linear_data)} записей")

with DAG(
    'xml_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    process_xml = PythonOperator(
        task_id='process_xml',
        python_callable=process_xml_file
    )
    
    process_xml
