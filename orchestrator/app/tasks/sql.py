from sqlalchemy import text
import duckdb
from typing import Optional
import logging

from prefect import task
from prefect.cache_policies import NO_CACHE

from app.constants import get_md_engine
from app.constants import TRANSACTIONS_LANDING_TABLE, LANDING_SCHEMA, STAGING_SCHEMA,TRANSACTIONS_STAGING_TABLE, SPACES_LANDING_TABLE, \
    SPACES_STAGING_TABLE , BALANCE_LANDING_TABLE, BALANCE_STAGING_TABLE, TRANSACTIONS_WEBHOOK_LANDING_TABLE
    
logger = logging.getLogger(__name__)

@task(cache_policy=NO_CACHE,task_run_name="execute_raw_sql-{label}")
def execute_raw_sql(sql: str, label: Optional[str] = None) -> None:
    engine = get_md_engine()
    try:
        with engine.begin() as connection:  # begin a transaction and auto-commit
            connection.execute(text(sql))
            logger.info(f"executed raw sql: {label}, SQL Snippet: {sql[:50]}")
    except Exception as e:
        logger.error(f"Error executing raw SQL: {label}, SQL Snippet: {sql[:50]} -->> Error {e}") 
        raise

@task(cache_policy=NO_CACHE, task_run_name="execute_transaction-{label}")
def execute_transaction(sql_statements: list[tuple[str, str]], label: Optional[str] = None) -> None:
    """
    Execute multiple SQL statements in a single transaction.
    
    Args:
        sql_statements: List of tuples (sql, description)
        label: Label for logging
    """
    engine = get_md_engine()
    
    try:
        with engine.begin() as connection:  # Single transaction for all statements
            for sql, description in sql_statements:
                logger.info(f"Executing: {description}")
                connection.execute(text(sql))
                logger.info(f"Completed: {description}")
            
            # If we get here, all succeeded - commit happens automatically
            logger.info(f"Transaction committed successfully: {label}")
            
    except Exception as e:
        # Rollback happens automatically on exception
        logger.error(f"Transaction failed and rolled back: {label} - Error: {e}")
        raise

create_lnd_schema = f"""
    CREATE SCHEMA IF NOT EXISTS {LANDING_SCHEMA};
"""
create_lnd_transactions_api_pull = f"""
    CREATE TABLE IF NOT EXISTS {LANDING_SCHEMA}.{TRANSACTIONS_LANDING_TABLE}
    (
      feedItemUid VARCHAR,
      categoryUid VARCHAR,
      direction VARCHAR,
      updatedAt VARCHAR,
      transactionTime VARCHAR,
      settlementTime VARCHAR,
      source VARCHAR,
      sourceSubType VARCHAR,
      status VARCHAR,
      transactingApplicationUserUid VARCHAR,
      counterPartyType VARCHAR,
      counterPartyUid VARCHAR,
      counterPartyName VARCHAR,
      counterPartySubEntityUid VARCHAR,
      reference VARCHAR,
      country VARCHAR,
      spendingCategory VARCHAR,
      userNote VARCHAR,
      hasAttachment BOOLEAN,
      hasReceipt BOOLEAN,
      batchPaymentDetails VARCHAR,
      "amount.currency" VARCHAR,
      "amount.minorUnits" BIGINT,
      "sourceAmount.currency" VARCHAR,
      "sourceAmount.minorUnits" BIGINT,
      counterPartySubEntityName VARCHAR,
      counterPartySubEntityIdentifier VARCHAR,
      counterPartySubEntitySubIdentifier VARCHAR,
      received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
"""
create_lnd_spaces = f"""
    CREATE TABLE IF NOT EXISTS {LANDING_SCHEMA}.{SPACES_LANDING_TABLE}(
      savingsGoalUid VARCHAR,
      "name" VARCHAR,
      sortOrder BIGINT,
      state VARCHAR,
      "totalSaved.currency" VARCHAR,
      "totalSaved.minorUnits" BIGINT,
      received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
"""
truncate_lnd_transactions = f"""
    TRUNCATE TABLE {LANDING_SCHEMA}.{TRANSACTIONS_LANDING_TABLE};
"""
truncate_lnd_spaces = f"""
    TRUNCATE TABLE {LANDING_SCHEMA}.{SPACES_LANDING_TABLE};
"""
truncate_lnd_balance = f"""
    TRUNCATE TABLE {LANDING_SCHEMA}.{BALANCE_LANDING_TABLE};
"""
create_stg_schema = f"""
    CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};
"""
truncate_stg_transactions = f"""
    TRUNCATE TABLE {STAGING_SCHEMA}.{TRANSACTIONS_STAGING_TABLE};
"""
create_stg_transactions = f"""
    CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{TRANSACTIONS_STAGING_TABLE}
    (
        transaction_id UUID PRIMARY KEY,
        space_id UUID NOT NULL,
        in_or_out VARCHAR(10) NOT NULL,
        updated_at DATETIME ,
        transaction_time DATETIME NOT NULL,
        source_type NVARCHAR(200),
        counter_party_type NVARCHAR(250),
        counter_party_name NVARCHAR(250),
        reference NVARCHAR(250),
        country NVARCHAR(100),
        spending_category NVARCHAR(100),
        currency VARCHAR(10),
        amount DECIMAL(10,2),
        user_note VARCHAR(250),
        data_source VARCHAR(100),
        received_at DATETIME,
        last_modified DATETIME,
        last_modified_by VARCHAR(100)
    );
"""

truncate_stg_spaces = f"""
    TRUNCATE TABLE {STAGING_SCHEMA}.{SPACES_STAGING_TABLE};
"""

create_stg_transactions = f"""
    CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{SPACES_STAGING_TABLE} 
    (
        space_uuid UUID PRIMARY KEY,
        space_name VARCHAR,
        amount DECIMAL(10,2), 
        received_at DATETIME,
        last_modified DATETIME,
    );
"""
insert_transactions_to_staging = f"""
    INSERT INTO {STAGING_SCHEMA}.{TRANSACTIONS_STAGING_TABLE} (
        transaction_id,
        space_id,
        in_or_out,
        updated_at,
        transaction_time,
        source_type,
        counter_party_type,
        counter_party_name,
        reference,
        user_note,
        country,
        spending_category,
        currency,
        amount,
        status,
        received_at,
        data_source,
        last_modified
    )
    FROM {LANDING_SCHEMA}.{TRANSACTIONS_LANDING_TABLE}
    SELECT
        feedItemUid::UUID AS transaction_id,
        categoryUid::UUID AS space_id,
        LOWER(direction) AS in_or_out,
        updatedAt::DATETIME AS updated_at,
        transactionTime::DATETIME AS transaction_time,
        LOWER(REPLACE(sourceSubType, '_', ' ')) AS source_type,
        LOWER(REPLACE(counterPartyType, '_', ' ')) AS counter_party_type,
        LOWER(REPLACE(counterPartyName, '_', ' ')) AS counter_party_name,
        reference,
        userNote AS user_note,
        country,
        LOWER(REPLACE(spendingCategory, '_', ' ')) AS spending_category,
        "amount.currency" AS currency,
        ("amount.minorUnits"/ 100.0)::DECIMAL(10,2) AS amount,
        NULL AS status,
        received_at,
        'api_pull' AS data_source,
        CURRENT_TIMESTAMP AS last_modified
    ;
"""
insert_spaces_to_staging = f"""
    INSERT INTO {STAGING_SCHEMA}.{SPACES_STAGING_TABLE} (
           space_id, 
           space_name, 
           amount, 
           received_at, 
           last_modified
        )
        SELECT 
          savingsGoalUid, 
          name, 
          ("totalSaved.minorUnits"/ 100.0),
          received_at , 
          CURRENT_TIMESTAMP AS last_modified
        FROM {LANDING_SCHEMA}.{SPACES_LANDING_TABLE}
    ;
"""

truncate_stg_balance = f"""
    TRUNCATE TABLE stg.balance;
"""

insert_balance_to_staging = f"""
        INSERT INTO {STAGING_SCHEMA}.{BALANCE_STAGING_TABLE} (balance, balance_with_spaces, received_at, last_modified)
        SELECT
            ("effectiveBalance.minorUnits" / 100.0)::DECIMAL(19, 2) AS balance,
            ("totalClearedBalance.minorUnits" / 100.0)::DECIMAL(19, 2) AS balance_with_spaces,
            received_at ,
            CURRENT_TIMESTAMP AS last_modified   
        FROM {LANDING_SCHEMA}.{BALANCE_LANDING_TABLE};
"""
insert_webhook_transactions_to_staging = f"""
        MERGE INTO {STAGING_SCHEMA}.{TRANSACTIONS_STAGING_TABLE} AS t
            USING(
                SELECT
                    feedItemUid::UUID AS transaction_id,
                    categoryUid::UUID AS space_id,
                    LOWER(direction) AS in_or_out,
                    updatedAt::TIMESTAMP AS updated_at,
                    transactionTime::TIMESTAMP AS transaction_time,
                    'unavailable' AS source_type,
                    LOWER(REPLACE(counterPartyType, '_', ' ')) AS counter_party_type,
                    LOWER(REPLACE(counterPartyName, '_', ' ')) AS counter_party_name,
                    reference,
                    userNote AS user_note,
                    country,
                    LOWER(REPLACE(spendingCategory, '_', ' ')) AS spending_category,
                    amount_currency AS currency, 
                    (amount_minorUnits / 100.0)::DECIMAL(10,2) AS amount,
                    NULL::VARCHAR AS status,  
                    received_at,
                    'webhook' AS data_source,
                    last_modified, 
                    current_user() AS last_modified_by  
                FROM {LANDING_SCHEMA}.{TRANSACTIONS_WEBHOOK_LANDING_TABLE}
                WHERE last_modified > (SELECT COALESCE(MAX(last_modified),'1900-01-01'::TIMESTAMP) FROM {STAGING_SCHEMA}.{TRANSACTIONS_STAGING_TABLE})
            ) AS s
            ON s.transaction_id = t.transaction_id
            WHEN MATCHED THEN 
                UPDATE SET 
                    space_id = s.space_id,
                    in_or_out = s.in_or_out,
                    updated_at = s.updated_at,
                    transaction_time = s.transaction_time,
                    source_type = s.source_type,
                    counter_party_type = s.counter_party_type,
                    counter_party_name = s.counter_party_name,
                    reference = s.reference,
                    user_note = s.user_note,
                    country = s.country,
                    spending_category = s.spending_category,
                    currency = s.currency,
                    amount = s.amount,
                    status = s.status,
                    received_at = s.received_at,
                    data_source = s.data_source,
                    last_modified = s.last_modified,
                    last_modified_by = s.last_modified_by
            WHEN NOT MATCHED THEN 
                INSERT (transaction_id, space_id, in_or_out, updated_at, transaction_time, 
                        source_type, counter_party_type, counter_party_name, reference, user_note, 
                        country, spending_category, currency, amount, status, received_at, 
                        data_source, last_modified, last_modified_by)
                VALUES (s.transaction_id, s.space_id, s.in_or_out, s.updated_at, s.transaction_time,
                        s.source_type, s.counter_party_type, s.counter_party_name, s.reference, s.user_note,
                        s.country, s.spending_category, s.currency, s.amount, s.status, s.received_at,
                        s.data_source, s.last_modified, s.last_modified_by);
"""

if __name__ == "__main__":
    #execute_raw_sql(create_lnd_schema, label="Create Landing Schema")
    #execute_raw_sql(create_lnd_transactions_api_pull, label="Create Transactions Table")
    #execute_raw_sql(create_stg_schema, label="Create staging Schema")
    #execute_raw_sql(create_stg_transactions, label="Create staging Transactions Table")
    #execute_raw_sql(truncate_stg_transactions, label="truncate staging Transactions Table")
    #execute_raw_sql(insert_transactions_to_staging, label="insert transactions to staging from landing")
    #execute_transaction([
    #    (truncate_stg_balance , "truncate stg.balance "),
    #    (insert_balance_to_staging, "insert balance to staging from landing")
    #], label="balance to staging transaction")
        #execute_raw_sql(truncate_stg_balance , label="truncate stg.balance ")
        #execute_raw_sql(insert_balance_to_staging, label="insert balance to staging from landing")
    execute_raw_sql(insert_webhook_transactions_to_staging, label="Merge webhook transactions")

    
    