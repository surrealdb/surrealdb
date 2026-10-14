/**
[test]
reason = """
Setup for 3.00.00_index_datetime_collation.surql and its _current sibling: the
old binary writes an index over a Datetime field, stamped below
format version 4, whose entries spell each value as RFC 3339 text, with and
without fractional seconds."""
run = false
*/
DEFINE TABLE ts_test SCHEMALESS;
DEFINE INDEX idx_ts ON ts_test FIELDS ts;

CREATE ts_test:0 SET ts = d'2026-08-28T09:00:00Z';
CREATE ts_test:1 SET ts = d'2026-08-28T09:00:00.152Z';
CREATE ts_test:2 SET ts = d'2026-08-28T09:00:00.611Z';
CREATE ts_test:3 SET ts = d'2026-08-28T09:00:00.917Z';
CREATE ts_test:4 SET ts = d'2026-08-28T09:00:01.070Z';
