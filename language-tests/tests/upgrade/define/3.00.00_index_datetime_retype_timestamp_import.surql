/**
[test]
reason = """
Setup for 3.00.00_index_datetime_retype_timestamp.surql and its _current
sibling: the old binary writes a non-unique and a UNIQUE index over a column
declared `datetime`, with whole-second, fractional and pre-epoch values, so the
new binary keeps serving ranges from entries the old writer laid out after the
column is retyped to `timestamp`."""
run = false
*/
DEFINE TABLE ti SCHEMAFULL;
DEFINE FIELD ts ON ti TYPE datetime;
DEFINE INDEX ti_ts ON ti FIELDS ts;
CREATE ti:1 SET ts = d'1969-12-31T23:59:59.5Z';
CREATE ti:2 SET ts = d'2026-10-27T00:00:00Z';
CREATE ti:3 SET ts = d'2026-10-27T12:00:00Z';
CREATE ti:4 SET ts = d'2026-10-27T12:00:00.000001Z';
CREATE ti:5 SET ts = d'2026-10-28T00:00:00Z';
CREATE ti:6 SET ts = d'2030-01-01T00:00:00Z';

DEFINE TABLE tu SCHEMAFULL;
DEFINE FIELD ts ON tu TYPE datetime;
DEFINE INDEX tu_ts ON tu FIELDS ts UNIQUE;
CREATE tu:1 SET ts = d'2026-10-27T00:00:00Z';
CREATE tu:2 SET ts = d'2026-10-27T12:00:00Z';
CREATE tu:3 SET ts = d'2026-10-28T00:00:00Z';
