from faker import Faker
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUCTS = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor', 'Keyboard']
REGIONS  = ['North', 'South', 'East', 'West', 'Central']
STATUSES = ['completed', 'pending', 'cancelled']

def generate_order():
    return {
        "order_id":    fake.uuid4(),
        "customer_id": fake.uuid4(),
        "product":     random.choice(PRODUCTS),
        "quantity":    random.randint(1, 10),
        "price":       round(random.uniform(9.99, 999.99), 2),
        "region":      random.choice(REGIONS),
        "status":      random.choice(STATUSES),
        "timestamp":   datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Producer started. Sending orders to Kafka...")
    while True:
        order = generate_order()
        producer.send('orders', value=order)
        print(f"Sent: {order['order_id']} | {order['product']} | ${order['price']}")
        time.sleep(0.5)