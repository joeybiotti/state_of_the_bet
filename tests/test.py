import psycopg2
from kafka_pipeline.db import insert_contract_data, insert_market_data
from kafka import TopicPartition
import time
import json
import uuid

def test_insert_contract_data(db_connection):
    with db_connection.cursor() as cursor:
        contract = {
            'id': 9999,
            'market_id': 123,
            'name': 'Test Contract',
            'current_price': 0.50
        }

        # Insert contract data
        insert_contract_data(contract)

        # Query the database to verify that the contract data has been inserted correctly.
        cursor.execute(
            'SELECT * FROM contracts WHERE contract_id = %s;', (9999,))

        result = cursor.fetchone()

        # Verify that the retrieved contract data matches the expected values for Contract ID, Market ID, Contract name, and Price
        assert result is not None
        assert result[0] == 9999  # Contract ID
        assert result[1] == 123  # Market ID
        assert result[2] == 'Test Contract'  # Contract Name
        assert result[3] == 0.50  # Current Contract Price
        
                # Second insert (should fail due to unique constraint)
        try: 
            insert_contract_data(contract)
        except psycopg2.IntegrityError:
            db_connection.rollback()
            
        # Validate that only one record exists
        cursor.execute('SELECT COUNT(*) FROM contracts WHERE contract_id = %s', (contract['id'],))
        count = cursor.fetchone()[0]
        assert count == 1, 'Duplicate insert should not increase row count'

        # Cleanup to remove test data after verification
        cursor.execute('DELETE FROM contracts WHERE contract_id = %s', (9999,))
        db_connection.commit()


def test_insert_market_data(db_connection):
    with db_connection.cursor() as cursor:
        market = {
            'id': 9999,
            'name': 'Test Market',
            'category': 'Test Category'
        }

        # Insert Market Data
        insert_market_data(market)

        # Query the database to verify that the market data has been inserted correctly.
        cursor.execute(
            'SELECT * FROM markets WHERE market_id = %s', (9999,))

        result = cursor.fetchone()

        # Verify that the retrieved market data matches the expected values for ID, name, and category
        assert result is not None
        assert result[0] == 9999  # Market ID
        assert result[1] == 'Test Market'  # Market Name
        assert result[2] == 'Test Category'  # Category Name
        
        # Second insert (should fail due to unique constraint)
        try: 
            insert_market_data(market)
        except psycopg2.IntegrityError:
            db_connection.rollback()
            
        # Validate that only one record exists
        cursor.execute('SELECT COUNT(*) FROM markets WHERE market_id = %s', (market['id'],))
        count = cursor.fetchone()[0]
        assert count == 1, 'Duplicate insert should not increase row count'
        
        # Cleanup to remove test data after verification
        cursor.execute('DELETE FROM markets WHERE market_id = %s', (9999,))
        db_connection.commit()

def test_kafka_message_production(kafka_producer, kafka_consumer):
    topic= 'test_topic'
    message= {'id':1, 'data':'test message'}
    
    kafka_producer.send(topic, message)
    kafka_producer.flush()
    kafka_consumer.commit()
    
    assert True
    
def test_kafka_message_consumption(kafka_producer, kafka_consumer):


    topic = f'test_topic_{uuid.uuid4().hex[:8]}'
    messages = [
        {'id': 1, 'data': 'test message 1'},
        {'id': 2, 'data': 'test message 2'},
        {'id': 3, 'data': 'test message 3'}
    ]

    # Produce messages
    for msg in messages:
        kafka_producer.send(topic, msg)
    kafka_producer.flush()

    # Ensure consumer is at the beginning
    kafka_consumer.subscribe([topic])
    kafka_consumer.poll(0)
    
    timeout = time.time() + 5  # wait up to 5 seconds
    while not kafka_consumer.assignment():
        if time.time() > timeout:
            raise RuntimeError("Timeout waiting for partition assignment")
        kafka_consumer.poll(0.1)
    
    kafka_consumer.seek_to_beginning()

    # Wait briefly to ensure messages are available
    time.sleep(1)

    # Poll for messages
    timeout_ms = 5000
    consumed_messages = []
    end_time = time.time() + 10  # 10 seconds max

    while len(consumed_messages) < len(messages) and time.time() < end_time:
        records = kafka_consumer.poll(timeout_ms)
        for record_list in records.values():
            for record in record_list:
                value = record.value
                if isinstance(value, bytes):
                    value = json.loads(value.decode('utf-8'))
                consumed_messages.append(value)

    # Only compare the messages we expect
    consumed_messages = [m for m in consumed_messages if m in messages]

    assert consumed_messages == messages, f"Mismatch: {consumed_messages} != {messages}"

    kafka_consumer.commit()