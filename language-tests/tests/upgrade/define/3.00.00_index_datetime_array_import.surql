/**
[test]
reason = """
Setup for 3.00.00_index_datetime_array.surql: the old binary writes a
non-unique and a UNIQUE index over the elements of a Datetime array, so each
record owns one entry per element and the new binary maintains a fanned-out key
set the old writer laid out."""
run = false
*/
DEFINE TABLE arr SCHEMALESS;
DEFINE INDEX ix ON arr FIELDS ts.*;
CREATE arr:1 SET ts = [d'2026-08-28T09:00:00Z', d'2026-08-28T09:00:00.152Z'];
CREATE arr:2 SET ts = [d'2026-08-28T09:00:00.152Z', d'2026-08-28T09:00:02Z'];
CREATE arr:3 SET ts = [d'2026-08-28T09:00:01.070Z'];

DEFINE TABLE arru SCHEMALESS;
DEFINE INDEX ix ON arru FIELDS ts.* UNIQUE;
CREATE arru:1 SET ts = [d'2026-08-28T09:00:00Z', d'2026-08-28T09:00:00.152Z'];
CREATE arru:2 SET ts = [d'2026-08-28T09:00:02Z'];
