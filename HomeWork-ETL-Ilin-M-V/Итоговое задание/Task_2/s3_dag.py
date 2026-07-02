from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET_NAME = 'de-dataproc-bucket1'

def list_files():
    hook = S3Hook(aws_conn_id='yandex_s3_default')
    keys = hook.list_keys(bucket_name=BUCKET_NAME)
    if keys:
        for key in keys:
            print(key)
    return True

def upload_file():
    hook = S3Hook(aws_conn_id='yandex_s3_default')
    
    csv_data = """application_id,event_time,customer_id,region_code,product_type,requested_amount,term_months,credit_score,risk_level,decision_status,approved_amount,channel,employee_review_flag,processing_time_sec
1001,2024-01-01 10:00:00,CUST001,REG01,LOAN,5000,12,720,LOW,APPROVED,5000,ONLINE,Y,120
1002,2024-01-01 11:30:00,CUST002,REG02,CREDIT,10000,24,650,MEDIUM,REJECTED,0,BRANCH,N,85
1003,2024-01-01 14:15:00,CUST003,REG01,LOAN,7500,18,680,MEDIUM,APPROVED,7000,ONLINE,Y,150
1004,2024-01-02 09:00:00,CUST004,REG03,MORTGAGE,200000,360,750,LOW,APPROVED,180000,BROKER,Y,200
1005,2024-01-02 13:45:00,CUST005,REG02,CREDIT,15000,36,610,HIGH,REJECTED,0,ONLINE,N,95"""
    
    hook.load_string(
        string_data=csv_data,
        key="input/test_data.csv",
        bucket_name=BUCKET_NAME,
        replace=True
    )
    return True

def read_file():
    hook = S3Hook(aws_conn_id='yandex_s3_default')
    content = hook.read_key(
        key="input/test_data.csv",
        bucket_name=BUCKET_NAME
    )
    print(content)
    return True

def process_file():
    hook = S3Hook(aws_conn_id='yandex_s3_default')
    content = hook.read_key(
        key="input/test_data.csv",
        bucket_name=BUCKET_NAME
    )
    
    lines = content.strip().split('\n')
    
    approved = 0
    total = 0
    credit_scores = []
    processing_times = []
    
    for line in lines[1:]:
        if not line.strip():
            continue
        values = line.split(',')
        if len(values) < 14:
            print(f"Skipping malformed line: {line}")
            continue
        total += 1
        if values[9].strip() == 'APPROVED':
            approved += 1
        try:
            credit_scores.append(int(values[7].strip()))
            processing_times.append(int(values[13].strip()))
        except (ValueError, IndexError) as e:
            print(f"Error parsing line: {line}, error: {e}")
            continue
    
    if total > 0:
        approval_rate = (approved / total * 100)
        avg_credit_score = sum(credit_scores) / len(credit_scores) if credit_scores else 0
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        
        print(f"Approval rate: {approval_rate:.1f}%")
        print(f"Average credit score: {avg_credit_score:.0f}")
        print(f"Average processing time: {avg_processing_time:.1f} seconds")
    else:
        print("No valid data found")
    
    return True

default_args = {
    'owner': 'de_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='s3_dag',
    default_args=default_args,
    description='S3 operations with application data',
    schedule_interval='@daily',
    catchup=False,
    tags=['s3', 'yandex', 'applications'],
) as dag:

    list_task = PythonOperator(
        task_id='list_files',
        python_callable=list_files,
    )

    upload_task = PythonOperator(
        task_id='upload_file',
        python_callable=upload_file,
    )

    read_task = PythonOperator(
        task_id='read_file',
        python_callable=read_file,
    )
    
    process_task = PythonOperator(
        task_id='process_file',
        python_callable=process_file,
    )

    list_task >> upload_task >> read_task >> process_task