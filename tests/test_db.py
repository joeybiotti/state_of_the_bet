import pytest
import psycopg2
from kafka_pipeline.db import insert_contract_data, insert_market_data

@pytest.mark.parametrize("contract", [
    {'id': 9991, 'market_id': 123, 'name': 'Contract A', 'current_price': 1.23},
    {'id': 9992, 'market_id': 124, 'name': 'Contract B', 'current_price': 4.56},
])
def test_insert_contract_data(db_connection, contract):
    """Test inserting a contract record and verifying uniqueness constraint."""
    with db_connection.cursor() as cursor:
        insert_contract_data(contract)

        cursor.execute('SELECT * FROM contracts WHERE contract_id = %s;', (contract['id'],))
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == contract['id']
        assert result[1] == contract['market_id']
        assert result[2] == contract['name']
        assert result[3] == contract['current_price']

        try:
            insert_contract_data(contract)
        except psycopg2.IntegrityError:
            db_connection.rollback()

        cursor.execute('SELECT COUNT(*) FROM contracts WHERE contract_id = %s', (contract['id'],))
        count = cursor.fetchone()[0]
        assert count == 1

        cursor.execute('DELETE FROM contracts WHERE contract_id = %s;', (contract['id'],))
        db_connection.commit()


@pytest.mark.parametrize("market", [
    {'id': 9991, 'name': 'Market A', 'category': 'Politics'},
    {'id': 9992, 'name': 'Market B', 'category': 'Science'}
])
def test_insert_market_data(db_connection, market):
    """Test inserting a market record and verifying uniqueness constraint."""
    with db_connection.cursor() as cursor:
        insert_market_data(market)

        cursor.execute('SELECT * FROM markets WHERE market_id = %s;', (market['id'],))
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == market['id']
        assert result[1] == market['name']
        assert result[2] == market['category']

        try:
            insert_market_data(market)
        except psycopg2.IntegrityError:
            db_connection.rollback()

        cursor.execute('SELECT COUNT(*) FROM markets WHERE market_id = %s;', (market['id'],))
        count = cursor.fetchone()[0]
        assert count == 1

        cursor.execute('DELETE FROM markets WHERE market_id = %s;', (market['id'],))
        db_connection.commit()
