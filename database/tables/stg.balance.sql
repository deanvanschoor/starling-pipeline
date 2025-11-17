CREATE TABLE IF NOT EXISTS stg.balance (
  balance DECIMAL(19, 2),
  balance_with_spaces DECIMAL(19, 2),
  received_at DATETIME, 
  last_modified DATETIME,
);