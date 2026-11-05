/**
# `records-10000` with an empty `audit` table and no event: the control of the dormant-event
# write benches. `run = false`.

[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE audit SCHEMALESS;

	RETURN 'OK';
}
