# Итоговая работа по модулю 3
## ETL-процессы

**Студент:** Ильин Марат Викторович

---

## 1. Описание проекта
Репликация данных из MongoDB в PostgreSQL с помощью Apache Airflow.  
Созданы два DAG: для репликации данных и для построения аналитических витрин.

---

## 2. Используемые технологии
- Apache Airflow 2.9.0
- PostgreSQL 13
- MongoDB 6
- Docker / Docker Compose
- Python (pymongo, psycopg2)

## 3. Структура проекта
├── dags/
│ ├── replication_dag.py
│ └── analytics_dag.py 
├── scripts/
│ └── generate_data.js 
├── init_db/
│ └── 01-init.sql 
├── docker-compose.yml
├── Duplicate records
├── Result.jpg
└── README.md
---

## Запуск проекта

```bash
# 1. Запустить контейнеры
docker-compose up -d

# 2. Сгенерировать данные
docker cp scripts/generate_data.js etl-itog-mongodb-1:/tmp/generate_data.js
docker-compose exec mongodb mongosh /tmp/generate_data.js

# 3. Запустить репликацию
docker-compose exec airflow airflow dags trigger replication_dag

# 4. Проверить результат
docker-compose exec postgres psql -U airflow -d myapp_db -c "SELECT COUNT(*) FROM raw_data.user_sessions;"

# 5. Запустить витрины
docker-compose exec airflow airflow dags trigger analytics_dag

# 6. Проверить витрины
docker-compose exec postgres psql -U airflow -d myapp_db -c "SELECT * FROM dm.user_activity LIMIT 5;"
docker-compose exec postgres psql -U airflow -d myapp_db -c "SELECT * FROM dm.support;"
