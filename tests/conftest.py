import pytest
import psycopg2
import os


@pytest.fixture(scope='session')
def db_connection():
    conn = psycopg2.connect(
        db_name=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
    )
    cursor = conn.cursor()
    yield cursor
    conn.close()
