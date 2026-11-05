/**
# `records-10000-indexed-perms-full` plus a field-level SELECT permission on the
# indexed `number` field.
#
# The guard (`age < 200`) holds for every row, so a query returns the same
# values as against the unguarded arm. What changes is the plan: the indexed
# COUNT fast paths count index entries without ever reducing a document, so the
# planner declines them whenever the WHERE clause touches a field whose SELECT
# permission is not `Full` — otherwise a record user could learn the cardinality
# of values they may not read. The same guard also disqualifies the pre-decode
# WHERE filter and the TopK threshold probe, which read the field straight from
# revision-encoded bytes. `run = false`.

[env]
imports = ["./records-10000-indexed-perms-full.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD number ON record PERMISSIONS FOR select WHERE age < 200;

	RETURN 'OK';
}
