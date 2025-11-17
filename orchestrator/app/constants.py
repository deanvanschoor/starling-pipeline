from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from pathlib import Path

load_dotenv()

APP_DIR = Path(__file__).parent

STARLING_TOKEN = os.getenv('STARLING_TOKEN')
MOTHERDUCK_TOKEN = os.getenv('MD_TOKEN')
DATABASE = 'b_app'

def get_md_engine():
    """Create and return a new MotherDuck engine instance."""
    return create_engine(f'duckdb:///md:{DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}')

#landing
LANDING_SCHEMA = "lnd"
TRANSACTIONS_LANDING_TABLE = "transactions_api_pull"
TRANSACTIONS_WEBHOOK_LANDING_TABLE = "transactions_webhook"
SPACES_LANDING_TABLE = "spaces"
BALANCE_LANDING_TABLE = "balance"
#staging
STAGING_SCHEMA = "stg"
TRANSACTIONS_STAGING_TABLE = "transactions"
SPACES_STAGING_TABLE = "dim_spaces"
BALANCE_STAGING_TABLE = "balance"



