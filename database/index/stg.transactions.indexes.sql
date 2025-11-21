-- Duckdb creates indexes on PK, FK automatically
-- No need to create them manually
-- However, if you want to create additional indexes, you can do so here
-- Duckdb is a column database please see https://duckdb.org/docs/stable/guides/performance/indexing

CREATE INDEX idx_stg_transactions_space_id ON stg.transactions(space_id)
CREATE INDEX idx_stg_transactions_transaction_time ON stg.transactions(transaction_time);