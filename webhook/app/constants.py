from dotenv import load_dotenv
import duckdb
from pathlib import Path
import os

load_dotenv()

APP_DIR = Path(__file__).parent

MOTHERDUCK_TOKEN = os.getenv('motherduck_token')
DATABASE = 'b_app'
ACCOUNT_UUID = os.getenv('ACCOUNT_UUID')

def get_md_connection():
    md_connection = duckdb.connect(f'md:{DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}')
    return md_connection

if __name__ == "__main__":
    print("why you calling Constants bro?")