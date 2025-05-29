import pytest
from kafka_pipeline.db import insert_contract_data

def test_insert_contract_data(db_connection):
    contract = {
        'id':9999,
        'market_id': 123,
        'name': 'Test Contract',
        'current_price':0.50
    }
    
    insert_contract_data(contract)
    
    db_connection.execute('SELECT * FROM contracts WHERE contract_id = %s;', (9999,))

    result = db_connection.fetchone()
    
    assert result is not None
    assert result[0] == 9999 # Contract ID
    assert result[1] == 123 # Market ID
    assert result[2] == 'Test Contract'  # Contract Name
    assert result[3] == 0.50 # Current Contract Price