/**
[env]
imports = ["upgrade/define/3.00.00_index_datetime_compound_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = """
A compound index whose last column is a Datetime keeps working across the hop,
whichever spelling of a Datetime its keys hold. Every read here is paired with
its `WITH NOINDEX` twin and sorted, and asserts only what holds under either
spelling, so it passes on the old binary's index and on one this checkout
wrote: a seek on the whole tuple finds its row; after an update and a delete
of rows written before the hop, a seek on the values they held finds nothing,
and a UNIQUE index frees their slots, where an entry left behind would refuse
the next record that takes the value; and a UNIQUE index refuses a duplicate
tuple as written, at another precision and at another offset, while accepting
the same instant under another leading value."""

[[test.results]]
value = "[cmp_ts:1]"

[[test.results]]
value = "[cmp_ts:1]"

[[test.results]]
value = "[cmp_ts:2]"

[[test.results]]
value = "[cmp_ts:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[cmp_ts:1]"

[[test.results]]
value = "[cmp_ts:1]"

[[test.results]]
value = "[cmp_ts:3]"

[[test.results]]
value = "[cmp_ts:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[cmp_ts:5]"

[[test.results]]
value = "[cmp_ts:5]"

[[test.results]]
error = "Database index `ix` already contains ['x', d'2026-08-28T09:00:00Z'], with record `cmp_tsu:1`"

[[test.results]]
error = "Database index `ix` already contains ['x', d'2026-08-28T09:00:00Z'], with record `cmp_tsu:1`"

[[test.results]]
error = "Database index `ix` already contains ['x', d'2026-08-28T09:00:00Z'], with record `cmp_tsu:1`"

[[test.results]]
error = "Database index `ix` already contains ['x', d'2026-08-28T09:00:00.152Z'], with record `cmp_tsu:2`"

[[test.results]]
value = "[cmp_tsu:9]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[cmp_tsu:10]"

[[test.results]]
value = "[cmp_tsu:11]"

[[test.results]]
error = "Database index `ix` already contains ['x', d'2026-08-28T09:00:05Z'], with record `cmp_tsu:1`"

[[test.results]]
value = "[cmp_tsu:1]"

[[test.results]]
value = "[cmp_tsu:1]"

[[test.results]]
value = "[cmp_tsu:3]"

[[test.results]]
value = "[cmp_tsu:3]"

*/
-- Whole-tuple seeks on the non-unique index, whole second and fraction.
SELECT VALUE id FROM cmp_ts WITH INDEX ix WHERE a = 'x' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH NOINDEX WHERE a = 'x' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH INDEX ix WHERE a = 'x' AND ts = d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH NOINDEX WHERE a = 'x' AND ts = d'2026-08-28T09:00:00.152Z' ORDER BY id;

-- Move one pre-hop row to another instant and delete another.
UPDATE cmp_ts:1 SET ts = d'2026-08-28T09:00:05Z' RETURN NONE;
DELETE cmp_ts:2;

-- The values they held are held by no record now: the index and the scan agree.
SELECT VALUE id FROM cmp_ts WITH INDEX ix WHERE a = 'x' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH NOINDEX WHERE a = 'x' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH INDEX ix WHERE a = 'x' AND ts = d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH NOINDEX WHERE a = 'x' AND ts = d'2026-08-28T09:00:00.152Z' ORDER BY id;

-- The moved row is found at its new value, and the row under the other leading
-- value at the instant the first one left is untouched.
SELECT VALUE id FROM cmp_ts WITH INDEX ix WHERE a = 'x' AND ts = d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH NOINDEX WHERE a = 'x' AND ts = d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH INDEX ix WHERE a = 'y' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH NOINDEX WHERE a = 'y' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;

-- A row written now takes the freed tuple.
CREATE cmp_ts:5 SET a = 'x', ts = d'2026-08-28T09:00:00Z' RETURN NONE;
SELECT VALUE id FROM cmp_ts WITH INDEX ix WHERE a = 'x' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM cmp_ts WITH NOINDEX WHERE a = 'x' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;

-- UNIQUE: the same tuple again is refused as written, at another precision and
-- at another offset of the same instant.
CREATE cmp_tsu:9 SET a = 'x', ts = d'2026-08-28T09:00:00Z';
CREATE cmp_tsu:9 SET a = 'x', ts = d'2026-08-28T09:00:00.000Z';
CREATE cmp_tsu:9 SET a = 'x', ts = d'2026-08-28T11:00:00+02:00';
CREATE cmp_tsu:9 SET a = 'x', ts = d'2026-08-28T09:00:00.152Z';

-- The same instant under a new leading value is a different tuple.
CREATE cmp_tsu:9 SET a = 'z', ts = d'2026-08-28T09:00:00Z' RETURN VALUE id;

-- Move one pre-hop row and delete another: both tuples are free again, and the
-- moved row's new tuple is held.
UPDATE cmp_tsu:1 SET ts = d'2026-08-28T09:00:05Z' RETURN NONE;
DELETE cmp_tsu:2;
CREATE cmp_tsu:10 SET a = 'x', ts = d'2026-08-28T09:00:00Z' RETURN VALUE id;
CREATE cmp_tsu:11 SET a = 'x', ts = d'2026-08-28T09:00:00.152Z' RETURN VALUE id;
CREATE cmp_tsu:12 SET a = 'x', ts = d'2026-08-28T09:00:05Z';

-- Whole-tuple seeks on the UNIQUE index.
SELECT VALUE id FROM cmp_tsu WITH INDEX ix WHERE a = 'x' AND ts = d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM cmp_tsu WITH NOINDEX WHERE a = 'x' AND ts = d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM cmp_tsu WITH INDEX ix WHERE a = 'y' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM cmp_tsu WITH NOINDEX WHERE a = 'y' AND ts = d'2026-08-28T09:00:00Z' ORDER BY id;
