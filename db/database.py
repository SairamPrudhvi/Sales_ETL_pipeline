# db/database.py
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

def get_engine() -> Engine:
    """
    Creates and returns a SQLAlchemy Engine.
    This is the single source of truth for DB connections.
    """
    return create_engine(DATABASE_URL, pool_pre_ping=True)
