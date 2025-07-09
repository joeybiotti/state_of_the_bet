import pytest
import psycopg2
from kafka_pipeline.db import insert_contract_data, insert_batch_contracts
from datetime import datetime, timedelta

def test_contract_transaction_success(db_connection):
    '''Verify contract data commits successfully inside transaction'''
    contract = {'id': 9701, 'market_id': 911, 'name': 'Commit Test', 'current_price': 9.99}
    
    with db_connection.cursor() as cursor:
        insert_contract_data(contract)
        
        cursor.execute('SELECT * FROM contracts WHERE contract_id = %s',(contract['id'],))
        result = cursor.fetchone()
        assert result is not None 
        assert result[0] == contract['id']
        
        cursor.execute('DELETE FROM contracts WHERE contract_id = %s', (contract['id'],))
        db_connection.commit()