import json
from kafka import KafkaConsumer
from db import insert_market_data

KAFKA_TOPIC = 'predictit_markets'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commits=True,
    value_deserializer=lambda v: json.loads(v.decode('uft-8'))
)

# Process Incoming Kafka Messages


def consume_kafka_data():
    for message in consumer:
        market_data = message.value
        print(f'Received Market: {market_data}')  # Debugging

        insert_market_data(market_data)


if __name__ == '__main__':
    consume_kafka_data()
