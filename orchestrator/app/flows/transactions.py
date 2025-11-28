from prefect import flow
import logging

from app.tasks.api_calls import upload_13m_transactions,get_account_details
from app.tasks.sql import execute_raw_sql, execute_transaction, truncate_lnd_transactions, truncate_stg_transactions , insert_transactions_to_staging, insert_webhook_transactions_to_staging

logger = logging.getLogger(__name__)

@flow(name="refresh-landing-transactions", log_prints=True, description="Full refresh of landing transactions Table via api call",timeout_seconds=180)            
def refresh_lnd_transactions():
    execute_raw_sql(truncate_lnd_transactions, label="Truncate Landing Transactions Table")
    account_uid = get_account_details('accountUid')
    upload_13m_transactions(account_uid)
    
@flow(name="insert-transactions-to-staging-api", log_prints=True, description="Insert transactions from API landing to staging Table",timeout_seconds=180)
def insert_to_staging():
    execute_transaction([
        (truncate_stg_transactions , "Truncate Staging Transactions Table "),
        (insert_transactions_to_staging, "Insert transactions to staging from landing")
    ], label="spaces to staging table")
    
@flow(name="pipe-transactions-lnd-to-stg-api", log_prints=True, description="Pipeline: transaction from lnd to stg",timeout_seconds=360)    
def transactions_dag():
    refresh_lnd_transactions()
    insert_to_staging()
    
@flow(name="insert-transactions-to-staging-webhook", log_prints=True, description="Insert transactions from webhook landing to staging Table",timeout_seconds=60)
def insert_webhook_to_staging():
    execute_raw_sql(insert_webhook_transactions_to_staging, label="Merge webhook transactions")
    
if __name__ == "__main__":
    from app.utils.logging_config import setup_logging
    setup_logging()
    transactions_dag()