import os
import duckdb
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

APP_DIR = Path(__file__).parent

MOTHERDUCK_TOKEN = os.getenv('MD_TOKEN')
DATABASE = 'b_app'

def get_md_connection():
    md_connection = duckdb.connect(f'md:{DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}')
    return md_connection


if __name__ == "__main__":
    print("why you calling Constants bro?")

