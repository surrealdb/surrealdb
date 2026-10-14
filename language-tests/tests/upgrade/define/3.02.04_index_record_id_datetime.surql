/**
[env]
imports = ["upgrade/define/3.02.04_index_record_id_datetime_import.surql"]

[test]
upgrade = true
# The import writes a Datetime inside an array or object record id, which
# 3.0.0's parser rejects; 3.2.4 and 3.3.0 accept it.
introduced = "3.2.4"
reason = """
Every index entry ends with the id of the record it names, whatever fields the
index covers and whatever the index's format stamp. A record id holding a
Datetime is spelled there as text, as the old writer spelled it, so the new
binary's writes reach the entries the old one wrote: an update replaces its
record's entry rather than adding a second one, and a delete removes its entry.
After both, indexed reads agree with a table scan and name each record once."""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[reading:['London', d'2026-08-28T09:00:00.152Z'], reading:{ at: d'2026-08-28T09:00:02Z', k: 'x' }, reading:['London', d'2026-08-28T09:00:00Z']]"

[[test.results]]
value = "[reading:['London', d'2026-08-28T09:00:00.152Z'], reading:{ at: d'2026-08-28T09:00:02Z', k: 'x' }, reading:['London', d'2026-08-28T09:00:00Z']]"

[[test.results]]
value = "[reading:['London', d'2026-08-28T09:00:00.152Z']]"

[[test.results]]
value = "[{ count: 3 }]"

*/
-- Rename one record, touch an unindexed field on another, delete a third.
UPDATE reading:['London', d'2026-08-28T09:00:00Z'] SET name = 'c' RETURN NONE;
UPDATE reading:['London', d'2026-08-28T09:00:00.152Z'] SET other = 1 RETURN NONE;
DELETE reading:['Paris', d'2026-08-28T09:00:01Z'];

-- Oracle: a table scan in name order.
SELECT VALUE id FROM reading WITH NOINDEX ORDER BY name ASC;

-- The index in order: the renamed record only at its new name.
SELECT VALUE id FROM reading WITH INDEX idx_name ORDER BY name ASC;

-- An equality seek on the old name finds only the record still holding it.
SELECT VALUE id FROM reading WITH INDEX idx_name WHERE name = 'a';

-- A range over every name counts each surviving record once.
SELECT count() FROM reading WITH INDEX idx_name WHERE name >= 'a' GROUP ALL;
