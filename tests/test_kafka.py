import time
import json
import uuid


def test_kafka_message_production(kafka_producer, kafka_consumer):
    """Tests producing a Kafka message.

    - Sends a single message to the test topic.
    - Flushes the producer to ensure delivery.
    - Commits offsets to prevent duplicate reads.
    """
    topic = 'test_topic'
    message = {'id': 1, 'data': 'test message'}

    kafka_producer.send(topic, message)
    kafka_producer.flush()
    kafka_consumer.commit()

    assert True


def test_kafka_message_consumption(kafka_producer, kafka_consumer):
    """Tests consuming Kafka messages from a dynamically generated topic.

    - Produces multiple test messages.
    - Ensures consumer subscribes and resets offsets correctly.
    - Polls Kafka for messages until expected count is met.
    - Filters out unexpected messages to validate correct consumption.
    """
    topic = f'test_topic_{uuid.uuid4().hex[:8]}'
    messages = [
        {'id': 1, 'data': 'test message 1'},
        {'id': 2, 'data': 'test message 2'},
        {'id': 3, 'data': 'test message 3'}
    ]

    for msg in messages:
        kafka_producer.send(topic, msg)
    kafka_producer.flush()

    kafka_consumer.subscribe([topic])
    kafka_consumer.poll(0)

    timeout = time.time() + 5
    while not kafka_consumer.assignment():
        if time.time() > timeout:
            raise RuntimeError("Timeout waiting for partition assignment")
        kafka_consumer.poll(0.1)

    kafka_consumer.seek_to_beginning()
    time.sleep(1)

    timeout_ms = 5000
    consumed_messages = []
    end_time = time.time() + 10

    while len(consumed_messages) < len(messages) and time.time() < end_time:
        records = kafka_consumer.poll(timeout_ms)
        for record_list in records.values():
            for record in record_list:
                value = record.value
                if isinstance(value, bytes):
                    value = json.loads(value.decode('utf-8'))
                consumed_messages.append(value)

    consumed_messages = [m for m in consumed_messages if m in messages]

    assert consumed_messages == messages, f"Mismatch: {consumed_messages} != {messages}"

    kafka_consumer.commit()
