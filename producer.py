import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configurar el Producer apuntando a Kafka local
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'financial-expenses'

# Listas para simular datos corporativos reales
cost_centers = ['FIN-001', 'IT-002', 'OPS-003', 'MKT-004', 'HR-005']
categories = ['Software Licenses', 'Cloud Infrastructure', 'Office Supplies', 'Travel & Per Diem', 'Consulting Services']

print(f"🚀 Iniciando productor de Kafka para el tópico: {topic_name}. Presiona Ctrl+C para detener.")

try:
    while True:
        # Generar un registro de gasto aleatoriohttps://dbc-38e68e4b-e849.cloud.databricks.com/?autoLogin=true&account_id=7670dd3e-f34c-4c7a-8389-eaea41eda520&o=7474655801374844
        expense_record = {
            "transaction_id": f"TXN-{random.randint(10000, 99999)}",
            "cost_center_id": random.choice(cost_centers),
            "category": random.choice(categories),
            "amount": round(random.uniform(150.0, 8500.0), 2),
            "currency": "USD",
            "timestamp": datetime.now().isoformat()
        }
        
        # Enviar el evento al tópico de Kafka
        producer.send(topic_name, value=expense_record)
        print(f"Enviado: {expense_record}")
        
        # Esperar entre 1 y 3 segundos antes del siguiente evento
        time.sleep(random.uniform(1.0, 3.0))

except KeyboardInterrupt:
    print("\n🛑 Productor detenido por el usuario.")
    producer.close()