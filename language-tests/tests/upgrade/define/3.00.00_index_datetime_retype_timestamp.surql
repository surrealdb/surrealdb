/**
[env]
imports = ["upgrade/define/3.00.00_index_datetime_retype_timestamp_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
An index an older binary built over a `datetime` column keeps answering the
same reads once the column is retyped to `timestamp`, and keeps doing so after
a backfill rewrites its entries as timestamps. Reads against datetime keys that
no longer match are not an error but a silent miss, so each phase asserts the
rows. An index that cannot be read still returns these rows through a table
scan; the plans are asserted in the _current sibling.

A datetime and a timestamp compare by kind rank, not by instant, so a range read
is only meaningful while every row holds one kind. The ranges are asserted
before any row is written and again after the backfill, never in between."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[ti:3, ti:4, ti:5, ti:6]"

[[test.results]]
value = "[ti:1, ti:2]"

[[test.results]]
value = "[ti:3]"

[[test.results]]
value = "[tu:2]"

[[test.results]]
value = "[{ id: ti:2, ts: d'2026-10-27T00:00:00Z' }]"

[[test.results]]
value = "[ti:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ti:3, ti:4, ti:5, ti:6]"

[[test.results]]
value = "[ti:1, ti:2]"

[[test.results]]
value = "[ti:2, ti:3, ti:4, ti:5]"

[[test.results]]
value = "[ti:3]"

[[test.results]]
value = "[tu:2]"

[[test.results]]
error = "Database index `tu_ts` already contains ts'2026-10-27T12:00:00', with record `tu:2`"
*/
-- Retype the column; the index and its entries are not touched.
DEFINE FIELD OVERWRITE ts ON ti TYPE timestamp;
DEFINE FIELD OVERWRITE ts ON tu TYPE timestamp;
-- Reads from the entries the old writer laid out, with datetime bounds.
SELECT VALUE id FROM ti WHERE ts >= d'2026-10-27T12:00:00Z' ORDER BY id;
SELECT VALUE id FROM ti WHERE ts < d'2026-10-27T12:00:00Z' ORDER BY id;
SELECT VALUE id FROM ti WHERE ts = d'2026-10-27T12:00:00Z';
SELECT VALUE id FROM tu WHERE ts = d'2026-10-27T12:00:00Z';
-- A write maintains the index across the retype: the row is found at its new key and no longer at the old one.
UPDATE ti:2 SET ts = ts;
SELECT VALUE id FROM ti WHERE ts = ts'2026-10-27T00:00:00';
SELECT VALUE id FROM ti WHERE ts = d'2026-10-27T00:00:00Z';
-- Backfill rewrites every entry as a timestamp.
UPDATE ti SET ts = <timestamp> ts RETURN NONE;
UPDATE tu SET ts = <timestamp> ts RETURN NONE;
SELECT VALUE id FROM ti WHERE ts >= ts'2026-10-27T12:00:00' ORDER BY id;
SELECT VALUE id FROM ti WHERE ts < ts'2026-10-27T12:00:00' ORDER BY id;
SELECT VALUE id FROM ti WHERE ts > ts'1969-12-31T23:59:59.5' AND ts <= ts'2026-10-28T00:00:00' ORDER BY id;
SELECT VALUE id FROM ti WHERE ts = ts'2026-10-27T12:00:00';
SELECT VALUE id FROM tu WHERE ts = ts'2026-10-27T12:00:00';
-- The unique index still refuses a duplicate.
CREATE tu:4 SET ts = ts'2026-10-27T12:00:00';
