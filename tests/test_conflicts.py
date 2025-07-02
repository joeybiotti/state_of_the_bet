import pytest
import jsonschema
from kafka_pipeline.db import insert_contract_data

contract_schema = {
        "type": "object",
        "properties": {
        "id": {"type": "integer"},
        "market_id": {"type": "integer"},
        "name": {"type": "string"},
        "current_price": {"type": "number"}
    },
    "required": ["id", "market_id", "name", "current_price"]
}

@pytest.mark.parametrize("original, updated", [
    (
        {'id': 9300, 'market_id': 802, 'name': 'Upsert A', 'current_price': 1.0},
        {'id': 9300, 'market_id': 802, 'name': 'Upsert A+', 'current_price': 2.0}
    ),
    (
        {'id': 9301, 'market_id': 803, 'name': 'Upsert B', 'current_price': 3.0},
        {'id': 9301, 'market_id': 803, 'name': 'Upsert B+', 'current_price': 4.0}
    )
])
def test_contract_upsert_updates_fields(db_connection, original, updated):
    with db_connection.cursor() as cursor:
        insert_contract_data(original)
        insert_contract_data(updated)
        
        cursor.execute('SELECT name, current_price FROM contracts WHERE contract_id = %s;', (updated['id'],))
        results = cursor.fetchone()
        
        assert results == (original['name'], updated['current_price'])
        
        cursor.execute('DELETE FROM contracts WHERE contract_id = %s;', (updated['id'],))
        db_connection.commit()