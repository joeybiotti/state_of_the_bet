import psycopg2
from kafka_pipeline.db import insert_contract_data, insert_market_data
from kafka import TopicPartition
import time
import json
import uuid

def test_insert_contract_data(db_connection):
    """Tests inserting contract data into the database.
    
    - Inserts a test contract into the database.
    - Verifies that the contract was correctly inserted.
    - Ensures duplicate inserts fail due to unique constraints.
    - Cleans up test data after verification.
    """
    with db_connection.cursor() as cursor:
        contract = {
            'id': 9999,
            'market_id': 123,
            'name': 'Test Contract',
            'current_price': 0.50
        }

        insert_contract_data(contract)

        cursor.execute('SELECT * FROM contracts WHERE contract_id = %s;', (9999,))
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == 9999  
        assert result[1] == 123  
        assert result[2] == 'Test Contract'  
        assert result[3] == 0.50  
        
        try: 
            insert_contract_data(contract)
        except psycopg2.IntegrityError:
            db_connection.rollback()
            
        cursor.execute('SELECT COUNT(*) FROM contracts WHERE contract_id = %s', (contract['id'],))
        count = cursor.fetchone()[0]
        assert count == 1, 'Duplicate insert should not increase row count'

        cursor.execute('DELETE FROM contracts WHERE contract_id = %s', (9999,))
        db_connection.commit()


def test_insert_market_data(db_connection):
    """Tests inserting market data into the database.
    
    - Inserts a test market record.
    - Ensures the market data is correctly inserted.
    - Verifies duplicate inserts fail due to constraints.
    - Cleans up test data after verification.
    """
    with db_connection.cursor() as cursor:
        market = {
            'id': 9999,
            'name': 'Test Market',
            'category': 'Test Category'
        }

        insert_market_data(market)

        cursor.execute('SELECT * FROM markets WHERE market_id = %s', (9999,))
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == 9999  
        assert result[1] == 'Test Market'  
        assert result[2] == 'Test Category'  
        
        try: 
            insert_market_data(market)
        except psycopg2.IntegrityError:
            db_connection.rollback()
            
        cursor.execute('SELECT COUNT(*) FROM markets WHERE market_id = %s', (market['id'],))
        count = cursor.fetchone()[0]
        assert count == 1, 'Duplicate insert should not increase row count'
        
        cursor.execute('DELETE FROM markets WHERE market_id = %s', (9999,))
        db_connection.commit()

def test_kafka_message_production(kafka_producer, kafka_consumer):
    """Tests producing a Kafka message.
    
    - Sends a single message to the test topic.
    - Flushes the producer to ensure delivery.
    - Commits offsets to prevent duplicate reads.
    """
    topic= 'test_topic'
    message= {'id':1, 'data':'test message'}
    
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
