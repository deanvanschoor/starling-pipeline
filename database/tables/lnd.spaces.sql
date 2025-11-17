CREATE TABLE lnd.spaces(
  savingsGoalUid VARCHAR,
  "name" VARCHAR,
  sortOrder BIGINT,
  state VARCHAR,
  "totalSaved.currency" VARCHAR,
  "totalSaved.minorUnits" BIGINT,
  received_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);    