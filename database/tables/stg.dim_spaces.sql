CREATE TABLE stg.dim_spaces (
    space_uuid UUID PRIMARY KEY,
    space_name VARCHAR,
    amount DECIMAL(10,2), 
    received_at DATETIME,
    last_modified DATETIME,
);