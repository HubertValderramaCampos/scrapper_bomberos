import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

_conn = None

def conectar():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require",
    )

def get_conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = conectar()
    else:
        try:
            _conn.cursor().execute("SELECT 1")
        except Exception:
            _conn = conectar()
    return _conn

# backwards-compat alias so existing `from db import conn` still works
class _ConnProxy:
    def cursor(self):        return get_conn().cursor()
    def commit(self):        return get_conn().commit()
    def rollback(self):      return get_conn().rollback()
    @property
    def closed(self):        return get_conn().closed

conn = _ConnProxy()
