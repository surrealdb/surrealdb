/**
[env]
imports = ["upgrade/define/3.00.00_index_datetime_nested_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
reason = """
An index whose indexed value holds a Datetime below the top level of the record
keeps working across the hop, whichever spelling of a Datetime its keys hold:
one over a nested path (`meta.ts`) and one over a whole object. Every read here
is paired with its `WITH NOINDEX` twin and sorted, and asserts only what holds under either spelling: a seek on the value finds its
row; after an update and a delete of rows written before the hop, a seek on the
value they held finds nothing and a UNIQUE index frees its slot; and a UNIQUE
index refuses a duplicate as written, at another precision and at another
offset, while accepting a different value."""

[[test.results]]
value = "[nest:1]"

[[test.results]]
value = "[nest:1]"

[[test.results]]
value = "[nest:2]"

[[test.results]]
value = "[nest:2]"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[nest:1]"

[[test.results]]
value = "[nest:1]"

[[test.results]]
value = "[nest:3]"

[[test.results]]
value = "[nest:3]"

[[test.results]]
error = "Database index `ix_path` already contains d'2026-08-28T09:00:00Z', with record `nestu:1`"

[[test.results]]
error = "Database index `ix_path` already contains d'2026-08-28T09:00:00Z', with record `nestu:1`"

[[test.results]]
error = "Database index `ix_path` already contains d'2026-08-28T09:00:00Z', with record `nestu:1`"

[[test.results]]
error = "Database index `ix_obj` already contains { at: d'2026-08-28T09:00:00Z' }, with record `nestu:1`"

[[test.results]]
error = "Database index `ix_obj` already contains { at: d'2026-08-28T09:00:00Z' }, with record `nestu:1`"

[[test.results]]
error = "Database index `ix_obj` already contains { at: d'2026-08-28T09:00:00Z' }, with record `nestu:1`"

[[test.results]]
value = "[nestu:9]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[nestu:10]"

[[test.results]]
value = "[nestu:11]"

[[test.results]]
error = "Database index `ix_path` already contains d'2026-08-28T09:00:05Z', with record `nestu:1`"

[[test.results]]
error = "Database index `ix_obj` already contains { at: d'2026-08-28T09:00:05Z' }, with record `nestu:1`"

[[test.results]]
value = "[nestu:11]"

[[test.results]]
value = "[nestu:11]"

[[test.results]]
value = "[nestu:1]"

[[test.results]]
value = "[nestu:1]"

*/
-- Seeks on the nested path and the whole object.
SELECT VALUE id FROM nest WITH INDEX ix_path WHERE meta.ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM nest WITH NOINDEX WHERE meta.ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM nest WITH INDEX ix_obj WHERE obj = { at: d'2026-08-28T09:00:00.152Z' } ORDER BY id;
SELECT VALUE id FROM nest WITH NOINDEX WHERE obj = { at: d'2026-08-28T09:00:00.152Z' } ORDER BY id;

-- Update one pre-hop row in both values, and delete another.
UPDATE nest:1 SET meta.ts = d'2026-08-28T09:00:05Z', obj = { at: d'2026-08-28T09:00:05Z' } RETURN NONE;
DELETE nest:2;

-- The values they held are held by no record now.
SELECT VALUE id FROM nest WITH INDEX ix_path WHERE meta.ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM nest WITH NOINDEX WHERE meta.ts = d'2026-08-28T09:00:00Z' ORDER BY id;
SELECT VALUE id FROM nest WITH INDEX ix_path WHERE meta.ts = d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM nest WITH NOINDEX WHERE meta.ts = d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM nest WITH INDEX ix_obj WHERE obj = { at: d'2026-08-28T09:00:00Z' } ORDER BY id;
SELECT VALUE id FROM nest WITH NOINDEX WHERE obj = { at: d'2026-08-28T09:00:00Z' } ORDER BY id;
SELECT VALUE id FROM nest WITH INDEX ix_obj WHERE obj = { at: d'2026-08-28T09:00:00.152Z' } ORDER BY id;
SELECT VALUE id FROM nest WITH NOINDEX WHERE obj = { at: d'2026-08-28T09:00:00.152Z' } ORDER BY id;

-- The updated row is found at its new values, and the untouched row is still
-- found by the value it holds.
SELECT VALUE id FROM nest WITH INDEX ix_path WHERE meta.ts = d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM nest WITH NOINDEX WHERE meta.ts = d'2026-08-28T09:00:05Z' ORDER BY id;
SELECT VALUE id FROM nest WITH INDEX ix_obj WHERE obj = { at: d'2026-08-28T09:00:01.070Z' } ORDER BY id;
SELECT VALUE id FROM nest WITH NOINDEX WHERE obj = { at: d'2026-08-28T09:00:01.070Z' } ORDER BY id;

-- UNIQUE: a duplicate is refused as written, at another precision and at another
-- offset, on the nested path and on the whole object.
CREATE nestu:9 SET meta = { ts: d'2026-08-28T09:00:00Z' }, obj = { at: d'2026-08-28T09:00:07Z' };
CREATE nestu:9 SET meta = { ts: d'2026-08-28T09:00:00.000Z' }, obj = { at: d'2026-08-28T09:00:07Z' };
CREATE nestu:9 SET meta = { ts: d'2026-08-28T11:00:00+02:00' }, obj = { at: d'2026-08-28T09:00:07Z' };
CREATE nestu:9 SET meta = { ts: d'2026-08-28T09:00:07Z' }, obj = { at: d'2026-08-28T09:00:00Z' };
CREATE nestu:9 SET meta = { ts: d'2026-08-28T09:00:07Z' }, obj = { at: d'2026-08-28T11:00:00+02:00' };
CREATE nestu:9 SET meta = { ts: d'2026-08-28T09:00:07Z' }, obj = { at: d'2026-08-28T09:00:00.000Z' };

-- Different values are accepted.
CREATE nestu:9 SET meta = { ts: d'2026-08-28T09:00:07Z' }, obj = { at: d'2026-08-28T09:00:07Z' } RETURN VALUE id;

-- Update one pre-hop row and delete another: the values they held are free again.
UPDATE nestu:1 SET meta.ts = d'2026-08-28T09:00:05Z', obj = { at: d'2026-08-28T09:00:05Z' } RETURN NONE;
DELETE nestu:2;
CREATE nestu:10 SET meta = { ts: d'2026-08-28T09:00:00Z' }, obj = { at: d'2026-08-28T09:00:00Z' } RETURN VALUE id;
CREATE nestu:11 SET meta = { ts: d'2026-08-28T09:00:00.152Z' }, obj = { at: d'2026-08-28T09:00:00.152Z' } RETURN VALUE id;
CREATE nestu:12 SET meta = { ts: d'2026-08-28T09:00:05Z' }, obj = { at: d'2026-08-28T09:00:08Z' };
CREATE nestu:12 SET meta = { ts: d'2026-08-28T09:00:08Z' }, obj = { at: d'2026-08-28T09:00:05Z' };

-- Seeks on the UNIQUE indexes.
SELECT VALUE id FROM nestu WITH INDEX ix_path WHERE meta.ts = d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM nestu WITH NOINDEX WHERE meta.ts = d'2026-08-28T09:00:00.152Z' ORDER BY id;
SELECT VALUE id FROM nestu WITH INDEX ix_obj WHERE obj = { at: d'2026-08-28T09:00:05Z' } ORDER BY id;
SELECT VALUE id FROM nestu WITH NOINDEX WHERE obj = { at: d'2026-08-28T09:00:05Z' } ORDER BY id;
