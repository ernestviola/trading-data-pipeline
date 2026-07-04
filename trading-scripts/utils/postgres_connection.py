import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2


def postgres_connection():
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)

    conn = psycopg2.connect(os.getenv("DATABASE_URL"))

    return conn
