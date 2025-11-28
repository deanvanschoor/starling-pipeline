from prefect import flow
import logging

from app.tasks.api_calls import upload_spaces, get_account_details
from app.tasks.sql import execute_raw_sql, execute_transaction, truncate_lnd_spaces , truncate_stg_spaces, insert_spaces_to_staging

logger = logging.getLogger(__name__)

@flow(name="refresh-landing-spaces", log_prints=True, description="Full refresh of landing spaces table via api call",timeout_seconds=180)     
def refresh_lnd_spaces():
    account_uid = get_account_details('accountUid')
    execute_raw_sql(truncate_lnd_spaces, label="Trunacate Landing Spaces")
    upload_spaces(account_uid)
    
@flow(name="insert-spaces-to-staging", log_prints=True, description="Insert spaces from landing to staging Table",timeout_seconds=180)
def insert_to_spaces_staging():
    execute_transaction([
        (truncate_stg_spaces , "truncate stg.spaces "),
        (insert_spaces_to_staging, "insert spaces to staging from landing")
    ], label="spaces to staging table")
    
@flow(name="pipe-spaces-lnd-to-stg", log_prints=True, description="Pipeline: spaces from lnd to stg",timeout_seconds=360)    
def spaces_dag():
    refresh_lnd_spaces()
    insert_to_spaces_staging()
    
if __name__ == "__main__":
    from app.utils.logging_config import setup_logging
    setup_logging()
    spaces_dag()