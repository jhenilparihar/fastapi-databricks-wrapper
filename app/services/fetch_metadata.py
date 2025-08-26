from datetime import datetime
import psycopg2
from psycopg2 import sql
from app.core.config import (
    LAKEBASE_DB_NAME,
    LAKEBASE_USER,
    LAKEBASE_OAUTH_TOKEN,
    LAKEBASE_HOST,
)


def fetch_metadata():
    """
    Read metadata records from Lakebase
    """

    conn = psycopg2.connect(
        dbname=LAKEBASE_DB_NAME,
        user=LAKEBASE_USER,
        password=LAKEBASE_OAUTH_TOKEN,
        host=LAKEBASE_HOST,
        port="5432",
        sslmode="require",
    )
    print("Connected to Lakebase")
    cursor = conn.cursor()

    query = sql.SQL("SELECT * FROM {}.public.metadata").format(
        sql.Identifier(LAKEBASE_DB_NAME)
    )
    cursor.execute(query)
    rows = cursor.fetchall()
    print("Metadata log read")  
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return rows
