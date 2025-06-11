import psycopg2
from kafka_pipeline.db import insert_contract_data, insert_market_data


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
