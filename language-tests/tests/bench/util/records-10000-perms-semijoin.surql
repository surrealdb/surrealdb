/**
# Permissioned variant of `records-10000` whose table SELECT permission is a
# membership test against a subquery — `age IN (SELECT VALUE age FROM allow)`.
#
# The `allow` table holds exactly the ages 1..100, and every `record` row's
# `age` is in that range, so this arm returns the same 10k rows as the
# `records-10000-perms-where` arm whose permission is the plain predicate
# `age < 200`. The two differ only in how the predicate is executed: a scalar
# comparison per row versus a subquery evaluated per row. The subquery reads no
# field of the enclosing row, so an executor that recognises the shape can hoist
# it out of the row loop (or plan it as a semi-join) and evaluate `allow` once
# instead of 10,000 times.
#
# `allow` carries `PERMISSIONS FOR select FULL` because the subquery runs inside
# the permission clause under the same record-user session. `run = false`.

[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE user SCHEMALESS;
	CREATE user:bench SET name = 'bench';

	DEFINE ACCESS user ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM type::record('user', $id) )
		DURATION FOR TOKEN 1h, FOR SESSION 1h;

	-- Every age a `record` row can hold, so the membership test always passes.
	DEFINE TABLE allow SCHEMALESS PERMISSIONS FOR select FULL;
	FOR $i IN array::range(1, 101) {
		CREATE type::record('allow', $i) SET age = $i RETURN NONE;
	};

	ALTER TABLE record PERMISSIONS
		FOR select WHERE age IN (SELECT VALUE age FROM allow)
		FOR create, update, delete NONE;

	RETURN 'OK';
}
