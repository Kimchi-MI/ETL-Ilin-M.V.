from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_user_activity():
    conn = psycopg2.connect(
        host='postgres',
        database='myapp_db',
        user='myapp_user',
        password='myapp_password'
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dm.user_activity (
            user_id VARCHAR(50) PRIMARY KEY,
            total_sessions INTEGER,
            total_time_minutes NUMERIC(10,2),
            avg_session_length_minutes NUMERIC(10,2),
            unique_pages TEXT,
            unique_actions TEXT,
            preferred_device VARCHAR(20),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cur.execute("TRUNCATE TABLE dm.user_activity;")
    
    cur.execute("""
        INSERT INTO dm.user_activity 
        (user_id, total_sessions, total_time_minutes, avg_session_length_minutes, 
         unique_pages, unique_actions, preferred_device)
        SELECT 
            user_id,
            COUNT(*) as total_sessions,
            COALESCE(ROUND(SUM(EXTRACT(EPOCH FROM (end_time - start_time))/60)::numeric, 2), 0) as total_time_minutes,
            COALESCE(ROUND(AVG(EXTRACT(EPOCH FROM (end_time - start_time))/60)::numeric, 2), 0) as avg_session_length_minutes,
            MAX(pages_visited) as unique_pages,
            MAX(actions) as unique_actions,
            MODE() WITHIN GROUP (ORDER BY device) as preferred_device
        FROM raw_data.user_sessions
        GROUP BY user_id;
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def create_support():
    conn = psycopg2.connect(
        host='postgres',
        database='myapp_db',
        user='myapp_user',
        password='myapp_password'
    )
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dm.support (
            issue_type VARCHAR(50) PRIMARY KEY,
            total_tickets INTEGER,
            open_tickets INTEGER,
            closed_tickets INTEGER,
            avg_resolution_time_hours NUMERIC(10,2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cur.execute("TRUNCATE TABLE dm.support;")
    
    cur.execute("""
        INSERT INTO dm.support
        (issue_type, total_tickets, open_tickets, closed_tickets, avg_resolution_time_hours)
        SELECT 
            issue_type,
            COUNT(*) as total_tickets,
            COUNT(CASE WHEN status IN ('open', 'in_progress') THEN 1 END) as open_tickets,
            COUNT(CASE WHEN status = 'closed' THEN 1 END) as closed_tickets,
            COALESCE(ROUND(AVG(CASE WHEN status = 'closed' 
                          THEN EXTRACT(EPOCH FROM (updated_at - created_at))/3600 
                          ELSE NULL END)::numeric, 2), 0) as avg_resolution_time_hours
        FROM raw_data.support_tickets
        GROUP BY issue_type;
    """)
    
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    'analytics_dag',
    default_args=default_args,
    description='Create analytics',
    schedule_interval='@daily',
    catchup=False,
    tags=['analytics'],
) as dag:
    
    user_mart = PythonOperator(
        task_id='create_user_activity',
        python_callable=create_user_activity,
    )
    
    support_mart = PythonOperator(
        task_id='create_support_mart',
        python_callable=create_support,
    )
    
    [user_mart, support_mart]
