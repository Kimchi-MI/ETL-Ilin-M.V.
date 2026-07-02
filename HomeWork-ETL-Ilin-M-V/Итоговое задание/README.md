Домашнее задание: Практическая работа. Модуль 4 (экзамен)

Ильин Марат Викторович 1 курс ИД

Общее описание проекта
В рамках домашнего задания был реализован полный цикл обработки данных с использованием сервисов Yandex Cloud:
Data Transfer — перенос данных из YDB в Object Storage
Airflow + Data Processing — автоматизированная обработка данных
Kafka + Spark Streaming — потоковая обработка данных
DataLens — визуализация и дашборды

Задание 1. Работа с Yandex DataTransfer
Цель
Перенести данные из Managed Service for YDB в Object Storage с помощью Data Transfer.

Выполненные шаги
1. Создание базы данных YDB
Создана база данных ydb-database

В ней создана таблица transactions_v2

2. Подготовка данных
Создана таблица 

sql
CREATE TABLE transactions_v2
(
    call_id Utf8,
    call_time Timestamp,
    client_id Utf8,
    region_code Utf8,
    campaign_type Utf8,
    call_status Utf8,
    client_response Utf8,
    duration_sec Uint32,
    follow_up_required Bool,
    PRIMARY KEY (call_id)
);
Структура данных:

Поле	Тип	Описание
call_id	Utf8	Уникальный ID звонка
call_time	Timestamp	Время звонка
client_id	Utf8	ID клиента
region_code	Utf8	Код региона
campaign_type	Utf8	Тип кампании
call_status	Utf8	Статус звонка
client_response	Utf8	Ответ клиента
duration_sec	Uint32	Длительность (сек)
follow_up_required	Bool	Нужно ли перезвонить

3. Создание эндпоинтов
Эндпоинт	Тип	База данных	Статус
hadoop13	Источник	YDB	Создан
hadoop	Приёмник	Object Storage	Создан

4. Создание и запуск трансфера
Параметры трансфера:
Имя: Preview
Источник: hadoop13 (YDB)
Приёмник: hadoop (Object Storage)
Тип: Копирование

5. Результат
В бакете de-dataproc-bucket1 появились файлы:
part-1781708093-c21f969b.00000.csv (2.99 МБ)

Задание 2. Автоматизация с Apache Airflow + Data Processing
Цель
Автоматизировать обработку данных с использованием Airflow и PySpark в Yandex Data Processing.

1. Данные для обработки
Файл: application_data.csv
Размер: 50+ МБ
Структура:

Поле	Тип	Описание
application_id	String	ID заявки
event_time	String	Время события
customer_id	String	ID клиента
region_code	String	Код региона
product_type	String	Тип продукта
requested_amount	Integer	Запрошенная сумма
term_months	Integer	Срок кредита
credit_score	Integer	Кредитный рейтинг
risk_level	String	Уровень риска
decision_status	String	Статус решения
approved_amount	Integer	Одобренная сумма
channel	String	Канал
employee_review_flag	String	Флаг проверки
processing_time_sec	Integer	Время обработки

2. PySpark задание (spark_processing.py)
Основные этапы обработки:
Чтение данных из CSV с заданной схемой
Добавление вычисляемых полей:
risk_category — категория риска на основе credit_score
is_approved — флаг одобрения
processing_date — дата обработки
Агрегация: средние значения по product_type
Сохранение результатов:
/output/result/data/ — обработанные данные
/output/result/aggregated/ — агрегированные результаты

Ключевой код:

python
# Чтение данных
df = spark.read.option("header", "true").schema(schema).csv(input_path)

# Вычисляемые поля
df = df.withColumn("risk_category", when(col("credit_score") < 450, "High Risk")
                   .when((col("credit_score") >= 450) & (col("credit_score") < 600), "Medium Risk")
                   .otherwise("Low Risk"))

# Агрегация
agg_df = df.groupBy("product_type").agg(
    {"requested_amount": "avg", "credit_score": "avg", "processing_time_sec": "avg"}
)

# Сохранение
df.write.mode("overwrite").parquet(f"{output_path}/data")
agg_df.write.mode("overwrite").parquet(f"{output_path}/aggregated")

3. DAG в Airflow (s3_dag.py)
Задачи DAG:
Задача	Описание
list_files	Список файлов в бакете
upload_file	Загрузка тестовых данных
read_file	Чтение данных из бакета
process_file	Обработка и вывод статистики

Результаты обработки:
text
Approval rate: 60.0%
Average credit score: 682
Average processing time: 130.0 seconds

4. Результаты в бакете
de-dataproc-bucket1

Задание 3. Работа с Kafka + PySpark Streaming
Цель
Настроить потоковую обработку данных из Kafka в Yandex Data Processing.

1. Данные для отправки
Формат JSON:

json
{
    "application_id": "loan_784512",
    "customer": {
        "customer_id": "cust_441",
        "region": "DE-HE"
    },
    "loan": {
        "amount": 15000,
        "term_months": 36
    },
    "scoring": {
        "score": 712,
        "risk_level": "medium"
    },
    "documents": [
        {
            "type": "passport",
            "status": "verified"
        }
    ],
    "decision_status": "manual_review"
}
2. Producer (producer.py)
Характеристики:
Количество сообщений: 20,000
Формат: JSON
Размер данных: 20+ МБ
Топик: loan_applications

3. Spark Streaming (loan_stream_processor.py)
Схема данных:

python
json_schema = StructType([
    StructField("application_id", StringType(), True),
    StructField("customer", customer_schema, True),
    StructField("loan", loan_schema, True),
    StructField("scoring", scoring_schema, True),
    StructField("documents", documents_schema, True),
    StructField("decision_status", StringType(), True)
])

Потоковая обработка:
Чтение из Kafka топика
Распарсивание JSON в плоскую структуру
Добавление поля processed_at
Сохранение в S3 (формат Parquet)

Результат:
Данные сохраняются в /output/loans_raw/
Чекпоинты в /dataproc/checkpoints/final/

Задание 4. Визуализация в DataLens
Цель
Построить дашборды для визуализации загруженных данных.

Источник данных
Таблица в Yandex Query: loop1
Файл в S3: input/application_data.csv

