from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values

# Load env variables
load_dotenv()

# Postgres Connection Details
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')

conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST
)

cursor = conn.cursor()


def insert_market_data(market_data):
    cursor.execute(
        """
    INSERT INTO markets (market_id, name, category)
    VALUES (%s, %s, %s)
    ON CONFLICT (market_id) DO UPDATE
    SET name = EXCLUDED.name, category = EXCLUDED.category;
    """,
        (market_data['id'], market_data['name'],
         market_data.get('category', None))
    )
    conn.commit()


def insert_contract_data(contract_data):
    sql = """
        INSERT INTO contracts (contract_id, market_id, name, current_price, updated_date)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (contract_id) DO UPDATE
        SET current_price = EXCLUDED.current_price, updated_date = NOW();
    """
    values = (
        contract_data.get("id"),
        contract_data.get("market_id"),
        contract_data.get("name"),
        contract_data.get("current_price")
    )

    try:
        cursor.execute(sql, values)
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Postgres Error: {e}")

def insert_batch_contracts(contract_list):
    """Insert multiple contracts using psycopg2's execute_values with proper template."""
    sql = """
        INSERT INTO contracts (contract_id, market_id, name, current_price, updated_date)
        VALUES %s
        ON CONFLICT (contract_id) DO UPDATE
        SET current_price = EXCLUDED.current_price, updated_date = NOW();
    """

    values = [(c['id'], c['market_id'], c['name'], c['current_price']) for c in contract_list]

    try:
        execute_values(cursor, sql, values, template="(%s, %s, %s, %s, NOW())")
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Postgres Error: {e}")