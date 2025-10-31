# db.py
 
import os
import time
import psycopg2
from psycopg2 import OperationalError
import pandas as pd
from sqlalchemy import create_engine
 
USER = "testuser"
PASSWORD = "testpass"
DATABASE = "testdb"
HOST = "localhost"
PORT = 5432
 
 
def get_connection(retries=5, delay=3):
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=HOST,
                database=DATABASE,
                user=USER,
                password=PASSWORD,
                port=PORT
            )
            print("Connection established.")
            return conn
        except OperationalError as e:
            print("PostgreSQL not ready, retrying...", e)
            retries -= 1
            time.sleep(delay)
 
    raise Exception("Could not connect to database after several retries")
 
 
# 👇 AÑADIDO: funciones para usar SQLAlchemy y escribir DataFrames
def get_engine():
    """Crea una conexión con SQLAlchemy."""
    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    return create_engine(url)
 
 
def write_df_to_db(df: pd.DataFrame, table_name: str):
    """Guarda un DataFrame como tabla en PostgreSQL."""
    engine = get_engine()
    try:
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"✅ DataFrame guardado en la tabla '{table_name}' correctamente.")
    except Exception as e:
        print(f"❌ Error al guardar en la base de datos: {e}")
 