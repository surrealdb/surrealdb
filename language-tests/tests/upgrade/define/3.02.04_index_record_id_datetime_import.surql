/**
[test]
reason = """
Setup for 3.02.04_index_record_id_datetime.surql: the old binary writes an index
over a plain string field on records whose compound ids hold a Datetime, so
every entry ends with an id the old writer spelled."""
run = false
*/
DEFINE TABLE reading SCHEMALESS;
DEFINE INDEX idx_name ON reading FIELDS name;

CREATE reading:['London', d'2026-08-28T09:00:00Z'] SET name = 'a';
CREATE reading:['London', d'2026-08-28T09:00:00.152Z'] SET name = 'a';
CREATE reading:['Paris', d'2026-08-28T09:00:01Z'] SET name = 'b';
CREATE reading:{ at: d'2026-08-28T09:00:02Z', k: 'x' } SET name = 'b';
