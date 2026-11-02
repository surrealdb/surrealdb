/**
[test]
reason = """
Setup for 3.00.00_field_temporal_sql_datetime.surql: the old binary writes the
table a SQL client creates, with `datetime` columns, one of them optional and
one indexed, and a row, so the new binary keeps accepting what a SQL driver
sends for a `datetime` column."""
run = false
*/
DEFINE TABLE sq TYPE NORMAL SCHEMAFULL;
DEFINE FIELD created ON sq TYPE datetime;
DEFINE FIELD seen ON sq TYPE option<datetime>;
DEFINE INDEX sq_created ON sq FIELDS created;
CREATE sq:1 SET created = d'2026-10-27T08:00:00Z', seen = d'2026-10-27T09:00:00Z';
