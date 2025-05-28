import json
import requests
from kafka import KafkaProducer

KAFKA_TOPIC = 'predictit_markets'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(
        v).encode('utf-8')  # JSON Serialization
)

# Fetch data


def get_predictit_data():
    url = 'https://www.predictit.org/api/marketdata/all/'
    response = requests.get(url)

    if response.status_code == 200:
        try:
            data = response.json()
            return data['markets']
        except ValueError as e:
            print(f'JSON Parse Error: {e}')
            return None
    print(
        f'Failed to fetch PredictIt data. Status Code: {response.status_code}')
    return None


# Send Data to Kafka
def stream_to_kafka():
    data = get_predictit_data()
    if data:
        for market in data:
            if 'contracts' in market:
                for contract in market['contracts']:
                    for key, value in contract.items():
                        if isinstance(value, bytes):
                            contract[key] = value.decode('utf-8')

        producer.send(KAFKA_TOPIC, value=market)

        producer.flush()
        print(f'Sent {len(data)} markets to Kafka.')
    else:
        print('Failed to fetch  PredictIt data.')


if __name__ == '__main__':
    stream_to_kafka()
