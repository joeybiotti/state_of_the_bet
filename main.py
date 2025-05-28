import json
import requests
import xml.etree.ElementTree as ET
from kafka import KafkaProducer

KAFKA_TOPIC = 'predictit_markets'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Fetch PredictIt data


def get_predictit_data():
    url = 'https://www.predictit.org/api/marketdata/all/'
    response = requests.get(url)

    if response.status_code == 200:
        try:
            data = response.json()
            return data["markets"]
        except ValueError as e:
            print(f"JSON Parse Error: {e}")
            return None

    print(f"Failed to fetch PredictIt data. Status code: {response.status_code}")
    return None

# Send data to Kafka


def stream_to_kafka():
    data = get_predictit_data()
    if data:
        for market in data:
            for contract in market['contracts']:
                for key, value in contract.items():
                    if isinstance(value, bytes):
                        contract[key] = value.decode('utf-8')
            producer.send(KAFKA_TOPIC, value=market)

        print(f'Sent {len(data)} markets to Kafka.')

        producer.flush()
        producer.close()
    else:
        print('Failed to fetch PredictIt data.')


if __name__ == '__main__':
    stream_to_kafka()
