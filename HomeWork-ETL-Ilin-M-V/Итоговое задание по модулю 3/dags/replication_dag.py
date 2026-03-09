from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pymongo
import psycopg2
import json

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def replicate_user_sessions():
    mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
    mongo_db = mongo_client['myapp_db']
    collection = mongo_db['user_sessions']
    
    sessions = list(collection.find())
    
    if not sessions:
        print('No user sessions found')
        return
    
    conn = psycopg2.connect(
        host='postgres',
        database='myapp_db',
        user='myapp_user',
        password='myapp_password'
    )
    cur = conn.cursor()
    
    for session in sessions:
        if '_id' in session:
            del session['_id']
        
        if 'pages_visited' in session and isinstance(session['pages_visited'], list):
            session['pages_visited'] = json.dumps(session['pages_visited'])
        if 'actions' in session and isinstance(session['actions'], list):
            session['actions'] = json.dumps(session['actions'])
        
        cur.execute("""
            INSERT INTO raw_data.user_sessions 
            (session_id, user_id, start_time, end_time, pages_visited, device, actions)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING
        """, (
            session['session_id'],
            session['user_id'],
            session['start_time'],
            session['end_time'],
            session['pages_visited'],
            session['device'],
            session['actions']
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    mongo_client.close()

def replicate_support_tickets():

    mongo_client = pymongo.MongoClient('mongodb://mongodb:27017/')
    mongo_db = mongo_client['myapp_db']
    collection = mongo_db['support_tickets']
    
    tickets = list(collection.find())
    
    if not tickets:
        print('No support tickets found')
        return
    
    conn = psycopg2.connect(
        host='postgres',
        database='myapp_db',
        user='myapp_user',
        password='myapp_password'
    )
    cur = conn.cursor()
    
    for ticket in tickets:
        if '_id' in ticket:
            del ticket['_id']
        
        if 'messages' in ticket and isinstance(ticket['messages'], list):
            ticket['messages'] = json.dumps(ticket['messages'])
        
        cur.execute("""
            INSERT INTO raw_data.support_tickets 
            (ticket_id, user_id, status, issue_type, messages, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO NOTHING
        """, (
            ticket['ticket_id'],
            ticket['user_id'],
            ticket['status'],
            ticket['issue_type'],
            ticket['messages'],
            ticket['created_at'],
            ticket['updated_at']
        ))
    
    conn.commit()
    cur.close()
    conn.close()
    mongo_client.close()

with DAG(
    'replication_dag',
    default_args=default_args,
    description='Replicate data from MongoDB to PostgreSQL',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'replication'],
) as dag:
    
    replicate_sessions = PythonOperator(
        task_id='replicate_user_sessions',
        python_callable=replicate_user_sessions,
    )
    
    replicate_tickets = PythonOperator(
        task_id='replicate_support_tickets',
        python_callable=replicate_support_tickets,
    )
    
    replicate_sessions >> replicate_tickets
