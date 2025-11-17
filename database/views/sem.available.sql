CREATE OR REPLACE VIEW sem.available AS
FROM b_app.stg.balance
SELECT
	balance AS available_budget,
	balance_with_spaces AS available_total,