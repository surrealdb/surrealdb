/**
[test]
reason = """
Setup for 3.00.00_index_datetime_nested.surql: the old binary writes indexes
whose indexed value holds a Datetime below the top level of the record: a
nested path and a whole object, non-unique and UNIQUE, so the new binary
maintains keys the old writer laid out for a Datetime inside another value."""
run = false
*/
DEFINE TABLE nest SCHEMALESS;
DEFINE INDEX ix_path ON nest FIELDS meta.ts;
DEFINE INDEX ix_obj ON nest FIELDS obj;
CREATE nest:1 SET meta = { ts: d'2026-08-28T09:00:00Z', k: 1 }, obj = { at: d'2026-08-28T09:00:00Z' };
CREATE nest:2 SET meta = { ts: d'2026-08-28T09:00:00.152Z', k: 2 }, obj = { at: d'2026-08-28T09:00:00.152Z' };
CREATE nest:3 SET meta = { k: 3 }, obj = { at: d'2026-08-28T09:00:01.070Z' };

DEFINE TABLE nestu SCHEMALESS;
DEFINE INDEX ix_path ON nestu FIELDS meta.ts UNIQUE;
DEFINE INDEX ix_obj ON nestu FIELDS obj UNIQUE;
CREATE nestu:1 SET meta = { ts: d'2026-08-28T09:00:00Z' }, obj = { at: d'2026-08-28T09:00:00Z' };
CREATE nestu:2 SET meta = { ts: d'2026-08-28T09:00:00.152Z' }, obj = { at: d'2026-08-28T09:00:00.152Z' };
