import pytest
import psycopg2
from datetime import datetime, timedelta
from kafka_pipeline.db import insert_contract_data, insert_market_data


@pytest.mark.parametrize("contract", [
    {'id': 9501, 'market_id': 900, 'name': 'Duplicate A', 'current_price': 1.00},
    {'id': 9502, 'market_id': 901, 'name': 'Duplicate B', 'current_price': 2.00}
])
def test_contract_duplicate_handling(db_connection, contract):
    """Test duplicate handling for contracts with unique constraint on contract_id."""
    with db_connection.cursor() as cursor:
        insert_contract_data(contract)
        try:
            insert_contract_data(contract)
        except psycopg2.IntegrityError:
            db_connection.rollback()

        cursor.execute(
            'SELECT COUNT(*) FROM contracts WHERE contract_id = %s;', (contract['id'],))
        count = cursor.fetchone()[0]
        assert count == 1

        cursor.execute(
            'DELETE FROM contracts WHERE contract_id = %s;', (contract['id'],))
        db_connection.commit()


@pytest.mark.parametrize("market", [
    {'id': 9503, 'name': 'Duplicate Market A', 'category': 'Tech'},
    {'id': 9504, 'name': 'Duplicate Market B', 'category': 'Politics'}
])
def test_market_duplicate_handling(db_connection, market):
    """Test duplicate handling for markets with unique constraint on market_id."""
    with db_connection.cursor() as cursor:
        insert_market_data(market)
        try:
            insert_market_data(market)
        except psycopg2.IntegrityError:
            db_connection.rollback()

        cursor.execute(
            'SELECT COUNT(*) FROM markets WHERE market_id = %s;', (market['id'],))
        count = cursor.fetchone()[0]
        assert count == 1

        cursor.execute(
            'DELETE FROM markets WHERE market_id = %s;', (market['id'],))
        db_connection.commit()


@pytest.mark.parametrize("original, updated", [
    (
        {'id': 9601, 'market_id': 902,
            'name': 'Original Contract', 'current_price': 10.00},
        {'id': 9601, 'market_id': 902,
            'name': 'Updated Contract', 'current_price': 20.00}
    )
])
def test_contract_upsert_logic(db_connection, original, updated):
    """Test update vs insert logic using ON CONFLICT upsert for contracts."""
    with db_connection.cursor() as cursor:
        insert_contract_data(original)
        time_before = datetime.now()

        insert_contract_data(updated)

        cursor.execute(
            'SELECT name, current_price, updated_date FROM contracts WHERE contract_id = %s;', (updated['id'],))
        result = cursor.fetchone()

        assert result[0] == updated['name']
        assert result[1] == updated['current_price']
        assert result[2] >= time_before - timedelta(seconds=1)

        cursor.execute(
            'DELETE FROM contracts WHERE contract_id = %s;', (updated['id'],))
        db_connection.commit()
