import os
import warnings
from pathlib import Path
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
import snowflake.connector

# pandas' read_sql only tests against SQLAlchemy engines/connections or
# sqlite3 DBAPI2 connections, so it warns on any other DBAPI2 connection -
# including Snowflake's, which works fine with read_sql in practice. Scoped
# to this specific message so other, possibly-real, warnings still surface.
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)


def snowflake_connection(role: str, schema: str = None):
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)

    key_path = Path(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")).expanduser()
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

    with open(key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
        )

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=private_key_der,
        role=role,
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=schema,
    )

    return conn
