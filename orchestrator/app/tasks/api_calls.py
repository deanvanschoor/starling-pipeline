import requests
import logging
from typing import Any, Dict, List , Generator, Tuple
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import inspect

from prefect import task, flow

from app.constants import get_md_engine
from app.constants import LANDING_SCHEMA, TRANSACTIONS_LANDING_TABLE ,STARLING_TOKEN ,SPACES_LANDING_TABLE, BALANCE_LANDING_TABLE
from app.tasks.sql import execute_raw_sql, truncate_lnd_transactions , truncate_lnd_spaces

logger = logging.getLogger(__name__)

@task(task_run_name="get_account_details-{detail}")
def get_account_details(detail: str = None ,api_key: str = STARLING_TOKEN) -> List[Dict[str, Any]]:
    url = "https://api.starlingbank.com/api/v2/accounts"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response = response.json()
        logging.info("Fetched account details successfully.")
        if detail is None:
            return response['accounts']            
        elif detail not in response['accounts'][0]:
            logging.error(f"Detail '{detail}' not found in account data.")
            raise ValueError(f"Detail '{detail}' not found in account data.")
        else:
            account_detail = response['accounts'][0][f'{detail}']
            return account_detail
    else:
        logging.error(f"Error {response.status_code}: {response.text}")
        response.raise_for_status()

@task(task_run_name="get_transactions-from:{from_timestamp}-to:{to_timestamp}")
def get_transactions(
    account_uid : str,
    from_timestamp : datetime, 
    to_timestamp : datetime, 
    api_key : str =STARLING_TOKEN
) -> List[Dict[str, Any]]:
    
    url = f"https://api.starlingbank.com/api/v2/feed/account/{account_uid}/settled-transactions-between" 
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }  
    params = {
        'minTransactionTimestamp': from_timestamp,  
        'maxTransactionTimestamp': to_timestamp
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get("feedItems", [])
    else:
        logging.error(f"Error {response.status_code}: {response.text}")
        response.raise_for_status()

@task        
def generate_monthly_ranges(
    start: datetime,
    end: datetime
) -> Generator[Tuple[str, str], None, None]:
    """
    Yield monthly date ranges between start and end.
    """
    current = start.replace(day=1, tzinfo=timezone.utc)
    while current < end:
        next_month = current + relativedelta(months=1)
        yield (
            current.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            min(next_month, end).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        )
        current = next_month

@task            
def clean_transactions(df: pd.DataFrame, existing_cols: list):
    # Keep only columns that exist in the table
    try:
        df = df[[c for c in df.columns if c in existing_cols]]
        return df
    except KeyError as e:
        logger.error(f"Error cleaning transactions: {e}")
        return pd.DataFrame(columns=existing_cols)

@task    
def upload_13m_transactions(account_uid: str, months: int = 13):
    to_timestamp = datetime.now(timezone.utc)
    from_timestamp = to_timestamp - relativedelta(months=months)

    # Inspect table columns once
    try:
        md_engine = get_md_engine()    
        inspector = inspect(md_engine)
        existing_cols = [col['name'] for col in inspector.get_columns(table_name=TRANSACTIONS_LANDING_TABLE, schema=LANDING_SCHEMA)]
    except Exception as e:
        logger.error(f"Cannot reach landing table for inspection : {e}")
        raise

    for from_ts, to_ts in generate_monthly_ranges(from_timestamp, to_timestamp):
        transactions = get_transactions(account_uid, from_ts, to_ts)
        if not transactions:
            continue
        try:  
            df = pd.json_normalize(transactions)
            df = clean_transactions(df, existing_cols)
            df.to_sql(TRANSACTIONS_LANDING_TABLE, md_engine,schema=LANDING_SCHEMA, if_exists='append', index=False)
            logger.info(f"Uploaded {len(df)} transactions from {from_ts} to {to_ts}")
        except Exception as e:
            logger.error(f"Error uploading transactions from {from_ts} to {to_ts}: {e}")
            continue

@task
def get_spaces(
    account_uid : str,
    api_key : str =STARLING_TOKEN
) -> List[Dict[str, Any]]:
    
    url = f"https://api.starlingbank.com/api/v2/account/{account_uid}/spaces" 
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }  
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("savingsGoals", [])
    else:
        logging.error(f"Error {response.status_code}: {response.text}")
        response.raise_for_status()

@task    
def upload_spaces(account_uid: str):    
    try:
        md_engine = get_md_engine()    
        inspector = inspect(md_engine)
        existing_cols = [col['name'] for col in inspector.get_columns(table_name=SPACES_LANDING_TABLE, schema=LANDING_SCHEMA)]
    except Exception as e:
        logger.error(f"Cannot reach landing table for inspection : {e}")
        raise
    spaces = get_spaces(account_uid)
    if not spaces:
        logger.error(f"Error extracting spaces: {e}")
        raise ValueError("No spaces data found") 
    try:  
        df = pd.json_normalize(spaces)
        df = clean_transactions(df, existing_cols)
        df.to_sql(SPACES_LANDING_TABLE, md_engine,schema=LANDING_SCHEMA, if_exists='append', index=False)
        logger.info(f"Uploaded {len(df)} spaces")
    except Exception as e:
        logger.error(f"Error uploading spaces: {e}")
        raise

@task
def get_balance(
    account_uid : str,
    api_key : str = STARLING_TOKEN
) -> List[Dict[str, Any]]:
    
    url = f"https://api.starlingbank.com/api/v2/accounts/{account_uid}/balance" 
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'application/json'
    }  
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()#.get("savingsGoals", [])
    else:
        logging.error(f"Error {response.status_code}: {response.text}")
        response.raise_for_status()

@task    
def upload_balance(account_uid: str):    
    try:
        md_engine = get_md_engine()    
        inspector = inspect(md_engine)
        existing_cols = [col['name'] for col in inspector.get_columns(table_name=BALANCE_LANDING_TABLE, schema=LANDING_SCHEMA)]
    except Exception as e:
        logger.error(f"Cannot reach landing table for inspection : {e}")
        raise
    balance = get_balance(account_uid)
    if not balance:
        logger.error(f"Error extracting spaces: {e}")
        raise ValueError("No spaces data found") 
    try:  
        df = pd.json_normalize(balance)
        df = clean_transactions(df, existing_cols)
        df.to_sql(BALANCE_LANDING_TABLE, md_engine,schema=LANDING_SCHEMA, if_exists='append', index=False)
        logger.info(f"Uploaded {len(df)} balance")
    except Exception as e:
        logger.error(f"Error uploading balance: {e}")
        raise

if __name__ == "__main__":
    #from app.utils.logging_config import setup_logging
    #setup_logging()
    account_uid = get_account_details('accountUid')
    execute_raw_sql(truncate_lnd_spaces, label="Trunacate Landing Spaces")
    upload_balance(account_uid)
    pass