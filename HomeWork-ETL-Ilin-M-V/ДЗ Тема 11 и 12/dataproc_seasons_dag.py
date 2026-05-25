from datetime import datetime
from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_FOLDER_ID = "b1g9fgpd8jmg2f6gr3fp"
YC_SUBNET_ID = "e9b914q9k1uj6qlk0h5d"
SERVICE_ACCOUNT_ID = "ajea5tkkngouru2232s7"  

YC_SSH_PUBLIC_KEY = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDTw5sJpxFogt/ZsBPOm7xWFpgHYEl9JBRtqaeWpLx/MLTfdGb9MWz2Hex7r473ZOispDcitOtDuy0KuSmEph48FUO5EqS8z0KKOPPt4+oofWW117VTb4ewuWMD8Ql0F+DtaWn7Mt9MnxcDo0xk6Di1scm8kACVcPJoZkBikaUlulDEbOu3sdYBT17jd6BCAdf2aOs7vmzAMg1qwPQWRdGmVwkt+fpdTAHuzIwQFM+fPZjXdsDsCeLS5azCzyMUC2UoKTRflrG9zu29mTapFcbFYsQqg0AFs5QiFBgXBollHLi4yswJTFA9754AaWPvFKiryXBvDYep3ku7ZRouWXHc4hdrXVQjkTAj8kX3v4GuiUPkkIPOmwqqNjdIyEgrFOXDQffE2VtHbyttGW1K5QcBtrJJ5OOzeDuNjklaMvROo3VqyVio6QduZNkxiXiLs5sEUnjWat0xLfb34JiouxIebS4XdNFYcae4sOz2X0huBd2iILCGDHml16bDsj4rxm9jqP9HqNP2vbZLLsYX2x/5tCBjS9TOv5xWLSQQYQ4zsn3V1ooGlsFd4qBGHhI0sXsEtVKI7BJ4xlGbfh/hb55JvTOUAo19IRILMVt80DGZCZCLly+AEn0mg/Hp+mpeiZFZopWjPHbHpBeX+ePWvJ/JcpQ/NXoMwddWvGT6wMN9bw== i@maratilin.ru"

default_args = {
    'owner': 'student',
    'start_date': datetime(2026, 5, 25),
    'retries': 1,
}

with DAG(
    dag_id='final_seasons_processing',  # Новое имя, чтобы избежать кеширования
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        folder_id=YC_FOLDER_ID,
        cluster_name='final-spark-cluster',  # Уникальное имя
        cluster_description='Test cluster for seasons',
        service_account_id=SERVICE_ACCOUNT_ID,
        subnet_id=YC_SUBNET_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone='ru-central1-a',
        cluster_image_version='2.0.26',
        masternode_resource_preset='s2.small',  # Минимальный мастер
        masternode_disk_size=50,
        computenode_resource_preset='s2.small',  # Минимальный воркер
        computenode_disk_size=50,
        computenode_count=1,  # Всего 1 воркер
        services=['YARN', 'SPARK'],
        # ПРОПУСКАЕМ: s3_bucket, dataproc_machine_type, остальное по умолчанию
    )

    # 2. ЗАПУСК ЗАДАЧИ (путь к скрипту должен быть точным)
    run_spark = DataprocCreatePysparkJobOperator(
        task_id='run_spark_job',
        main_python_file_uri='s3a://airflow-logs-24601/scripts/process_seasons.py',
        cluster_id=create_cluster.output,
    )

    # 3. УДАЛЕНИЕ КЛАСТЕРА
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        cluster_id=create_cluster.output,
        trigger_rule='all_done',
    )

    create_cluster >> run_spark >> delete_cluster