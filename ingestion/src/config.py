"""
Environmental variable and paths logic.
"""
from dotenv import load_dotenv
import os

load_dotenv()

RAW_DATA_DIR = "data/raw/"

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PW = os.getenv("POSTGRES_PW")

JDBC_URL = (
    f"jdbc:postgresql://"
    f"{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/"
    f"{POSTGRES_DB}"
)