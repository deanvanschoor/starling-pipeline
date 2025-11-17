from prefect import flow
import logging

from app.tasks.api_calls import upload_balance, get_account_details
from app.tasks.sql import execute_raw_sql , execute_transaction, truncate_lnd_balance , truncate_stg_balance, insert_balance_to_staging

logger = logging.getLogger(__name__)

@flow(name="refresh-lnd-balance", log_prints=True, description="Full refresh of landing balance table via api call",timeout_seconds=180)     
def refresh_lnd_balance():
    account_uid = get_account_details('accountUid')
    execute_raw_sql(truncate_lnd_balance, label="Trunacate Landing Balance")
    upload_balance(account_uid)
    
@flow(name="insert-balance-to-staging", log_prints=True, description="Insert balance from landing to staging Table",timeout_seconds=180)
def insert_to_balance_staging():
    execute_transaction([
        (truncate_stg_balance , "truncate stg.balance "),
        (insert_balance_to_staging, "insert balance to staging from landing")
    ], label="balance to staging transaction")
    
@flow(name="pipe-balance-lnd-to-stg", log_prints=True, description="Pipeline: balance from lnd to stg",timeout_seconds=360)    
def balance_dag():
    refresh_lnd_balance()
    insert_to_balance_staging()
    
if __name__ == "__main__":
    from app.utils.logging_config import setup_logging
    setup_logging()
    balance_dag()