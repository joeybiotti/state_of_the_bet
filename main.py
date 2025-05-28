import json
import requests
from kafka import KafkaProducer

KAFKA_TOPIC = 'predictit_markets'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

# Fetch PredictIt data


def get_predictit_data():
    url = 'https://www.predictit.org/api/marketdata/all/'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

# Send data to Kafka


def stream_to_kafka():
    data = get_predictit_data()
    if data:
        for market in data.get('markets', []):
            producer.send(KAFKA_TOPIC, market)
        print(f'Sent {len(data['markets'])} markets to Kafka.')
    else:
        print('Failed to fetch PredictIt data.')


if __name__ == '__main__':
    stream_to_kafka()
