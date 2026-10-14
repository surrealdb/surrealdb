/**
[test]
reason = """
Setup for 3.00.00_index_datetime_compound.surql: the old binary writes a
non-unique and a UNIQUE index over (string, Datetime), with whole-second and
fractional values under one leading value and the same instant under another,
so the new binary maintains and reads a compound key set the old writer laid
out."""
run = false
*/
DEFINE TABLE cmp_ts SCHEMALESS;
DEFINE INDEX ix ON cmp_ts FIELDS a, ts;
CREATE cmp_ts:1 SET a = 'x', ts = d'2026-08-28T09:00:00Z';
CREATE cmp_ts:2 SET a = 'x', ts = d'2026-08-28T09:00:00.152Z';
CREATE cmp_ts:3 SET a = 'y', ts = d'2026-08-28T09:00:00Z';
CREATE cmp_ts:4 SET a = 'x', ts = d'2026-08-28T09:00:01.070Z';

DEFINE TABLE cmp_tsu SCHEMALESS;
DEFINE INDEX ix ON cmp_tsu FIELDS a, ts UNIQUE;
CREATE cmp_tsu:1 SET a = 'x', ts = d'2026-08-28T09:00:00Z';
CREATE cmp_tsu:2 SET a = 'x', ts = d'2026-08-28T09:00:00.152Z';
CREATE cmp_tsu:3 SET a = 'y', ts = d'2026-08-28T09:00:00Z';
