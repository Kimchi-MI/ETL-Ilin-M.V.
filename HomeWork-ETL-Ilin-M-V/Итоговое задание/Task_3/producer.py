from kafka import KafkaProducer
import json
import random
import time
import ssl

KAFKA_BROKER = "rc1a-7sth8d2a4ltacuri.mdb.yandexcloud.net:9091"
TOPIC = "loan_applications"
COUNT = 20000

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SASL_SSL",
    ssl_cafile="/usr/share/ca-certificates/YandexInternalRootCA.crt",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username="producer-user",
    sasl_plain_password="Maratka10_",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

regions = ["DE-HE", "US-CA", "GB-LON", "FR-PAR", "ES-MAD", "IT-ROM", "NL-AMS"]
risk_levels = ["low", "medium", "high"]
doc_types = ["passport", "id_card", "drivers_license", "utility_bill"]
statuses = ["verified", "pending", "rejected"]
decision_statuses = ["approved", "manual_review", "rejected"]


for i in range(COUNT):
    message = {
        "application_id": f"loan_{i:08d}",
        "customer": {
            "customer_id": f"cust_{random.randint(100, 999)}",
            "region": random.choice(regions)
        },
        "loan": {
            "amount": random.randint(5000, 50000),
            "term_months": random.choice([12, 24, 36, 48, 60])
        },
        "scoring": {
            "score": random.randint(350, 850),
            "risk_level": random.choice(risk_levels)
        },
        "documents": [
            {
                "type": random.choice(doc_types),
                "status": random.choice(statuses)
            }
        ],
        "decision_status": random.choice(decision_statuses)
    }
    producer.send(TOPIC, value=message)

producer.flush()
producer.close()

