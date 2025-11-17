CREATE TABLE IF NOT EXISTS stg.transactions
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
        status VARCHAR(250),
        data_source VARCHAR(100),
        received_at DATETIME,
        last_modified DATETIME,
        last_modified_by VARCHAR(100)
    );