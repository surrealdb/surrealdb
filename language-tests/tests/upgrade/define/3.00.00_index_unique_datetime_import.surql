/**
[test]
reason = """
Setup for 3.00.00_index_unique_datetime.surql: the old binary writes a UNIQUE
index over a Datetime field, and records holding a whole second and a fraction
of it, so each value sits at a unique slot the old writer spelled."""
run = false
*/
DEFINE TABLE ts_unique SCHEMALESS;
DEFINE INDEX uniq_at ON ts_unique FIELDS at UNIQUE;

CREATE ts_unique:1 SET at = d'2026-08-28T09:00:00Z';
CREATE ts_unique:2 SET at = d'2026-08-28T09:00:00.152Z';
