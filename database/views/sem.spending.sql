-- sem.spending view
CREATE OR REPLACE VIEW sem.spending AS
FROM b_app.stg.transactions t
    LEFT JOIN b_app.stg.dim_spaces s
        ON t.space_id = s.space_id
    LEFT JOIN stg.dim_date d 
        ON t.transaction_time::DATE = d.date_key

SELECT
    COALESCE(s.space_name, 'Default') AS space,
    t.spending_category,
    t.counter_party_name AS spent_at,
    t.reference AS spending_reference,
    t.user_note,
    t.amount,
    t.transaction_time,
    d.month_abbr,
    d.year_month,

WHERE t.in_or_out = 'out'
AND t.spending_category NOT IN ('saving','none')
ORDER BY t.transaction_time DESC