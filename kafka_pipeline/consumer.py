import json
from kafka import KafkaConsumer
from db import insert_market_data, insert_contract_data

KAFKA_TOPIC = 'predictit_markets'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

# Process Incoming Kafka Messages


def consume_kafka_data():
    for message in consumer:
        market_data = message.value
        print(
            f"Contracts Extracted: {json.dumps(market_data.get('contracts', []), indent=2)}")

        insert_market_data(market_data)

        for contract in market_data.get('contracts', []):
            print(f'Received Contracts: {contract}')
            insert_contract_data(contract)


if __name__ == '__main__':
    consume_kafka_data()
