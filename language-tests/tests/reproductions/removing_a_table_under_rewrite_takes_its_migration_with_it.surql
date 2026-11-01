/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
`REMOVE TABLE` of a table whose `MIGRATE EXISTING ROWS` rewrite has not finished
succeeds, and takes the rows and the rewrite with it: a table defined again
under the name holds no rows and has no migration under way. The removal holds
the table migration job off the table's rows until its transaction ends, so a
page the job commits meanwhile cannot fail the removal's key-by-key delete of
them. The harness runs no maintenance job, so the race itself is pinned by
`kvs::tests::table_migration_test::removing_a_table_under_rewrite_holds_the_job_off_its_rows`;
this file pins what the statement leaves behind.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE sensor SCHEMAFULL;
DEFINE FIELD battery ON sensor TYPE int;
CREATE |sensor:1..2000| SET battery = 50 RETURN NONE;
ALTER FIELD battery ON sensor TYPE float
	MIGRATE EXISTING ROWS AS battery_fraction USING <float>$value / 100f FALLBACK 0f;
-- The rewrite is queued and has not run: no job runs in this harness.
(INFO FOR TABLE sensor).migrations.progress != NONE;
REMOVE TABLE sensor;
DEFINE TABLE sensor SCHEMAFULL;
DEFINE FIELD battery ON sensor TYPE int;
SELECT * FROM sensor;
(INFO FOR TABLE sensor).migrations.progress;
