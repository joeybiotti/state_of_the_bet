import pytest
import psycopg2
import os
from kafka import KafkaProducer,KafkaConsumer
import json

@pytest.fixture(scope='session')
def db_connection():
    conn = psycopg2.connect(
        dbname = os.getenv('DB_NAME'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASSWORD'),
        host = os.getenv('DB_HOST'),
    )
    yield conn
    conn.close()
  
@pytest.fixture
def kafka_producer():
     producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
     yield producer 
     producer.close()
    
@pytest.fixture
def kafka_consumer():
    consumer = KafkaConsumer(
        'test_topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    yield consumer
    consumer.close()