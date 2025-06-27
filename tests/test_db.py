import pytest
import psycopg2
from kafka_pipeline.db import insert_contract_data, insert_market_data, insert_batch_contracts, insert_batch_market_data


@pytest.mark.parametrize("contract", [
    {'id': 9991, 'market_id': 123, 'name': 'Contract A', 'current_price': 1.23},
    {'id': 9992, 'market_id': 124, 'name': 'Contract B', 'current_price': 4.56},
])
def test_insert_contract_data(db_connection, contract):
    """Test inserting a contract record and verifying uniqueness constraint."""
    with db_connection.cursor() as cursor:
        insert_contract_data(contract)

        cursor.execute(
            'SELECT * FROM contracts WHERE contract_id = %s;', (contract['id'],))
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

        cursor.execute(
            'SELECT COUNT(*) FROM contracts WHERE contract_id = %s', (contract['id'],))
        count = cursor.fetchone()[0]
        assert count == 1

        cursor.execute(
            'DELETE FROM contracts WHERE contract_id = %s;', (contract['id'],))
        db_connection.commit()


@pytest.mark.parametrize("market", [
    {'id': 9991, 'name': 'Market A', 'category': 'Politics'},
    {'id': 9992, 'name': 'Market B', 'category': 'Science'}
])
def test_insert_market_data(db_connection, market):
    """Test inserting a market record and verifying uniqueness constraint."""
    with db_connection.cursor() as cursor:
        insert_market_data(market)

        cursor.execute(
            'SELECT * FROM markets WHERE market_id = %s;', (market['id'],))
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == market['id']
        assert result[1] == market['name']
        assert result[2] == market['category']

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


@pytest.mark.parametrize("contracts", [
    [
        {'id': 7001, 'market_id': 800, 'name': 'Bulk A', 'current_price': 2.5},
        {'id': 7002, 'market_id': 801, 'name': 'Bulk B', 'current_price': 3.5}
    ],
    [
        {'id': 7003, 'market_id': 802, 'name': 'Bulk C', 'current_price': 4.5},
        {'id': 7004, 'market_id': 803, 'name': 'Bulk D', 'current_price': 5.5}
    ]
])
def test_insert_batch_contract_data(db_connection, contracts):
    '''Test batch insert of multiple contract records using insert_batch_contracts().'''
    with db_connection.cursor()as cursor:
        insert_batch_contracts(contracts)

        ids = tuple(c['id'] for c in contracts)
        cursor.execute(
            'SELECT COUNT(*) FROM contracts WHERE contract_id IN %s;', (ids,))
        count = cursor.fetchone()[0]
        assert count == len(contracts)

        cursor.execute(
            'DELETE FROM contracts WHERE contract_id IN %s;', (ids,))
        db_connection.commit()


@pytest.mark.parametrize("markets", [
    [
        {'id': 9001, 'name': 'Market A', 'category': 'Category X'},
        {'id': 9002, 'name': 'Market B', 'category': 'Category Y'}
    ],
    [
        {'id': 9003, 'name': 'Market C', 'category': 'Category Z'},
        {'id': 9004, 'name': 'Market D', 'category': 'Category W'}
    ]
])
def test_insert_batch_market_data(db_connection, markets):
    """Test batch insert of multiple market records using insert_batch_market_data()."""
    with db_connection.cursor() as cursor:
        insert_batch_market_data(markets)

        ids = tuple(m['id'] for m in markets)
        cursor.execute(
            'SELECT COUNT(*) FROM markets WHERE market_id IN %s;', (ids,))
        count = cursor.fetchone()[0]
        assert count == len(markets)

        cursor.execute(
            'DELETE FROM markets WHERE market_id IN %s;', (ids,))
        db_connection.commit()
