/**
# `records-10000-perms-full` plus eight field-level SELECT permissions.
#
# Same always-true guard as `records-10000-fieldperms-one` (`age < 200`, and
# `age` is 1..100), so this arm also returns the same documents as the
# unguarded `perms-full` arm — what scales with it is the number of guarded
# paths, each of which is expanded and judged per row. Paired with
# `records-10000-fieldperms-one` to separate the fixed cost of enforcing field
# permissions at all from the cost that grows with the number of guarded
# fields. `run = false`.

[env]
imports = ["./records-10000-perms-full.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD number ON record PERMISSIONS FOR select WHERE age < 200;
	DEFINE FIELD name ON record PERMISSIONS FOR select WHERE age < 200;
	DEFINE FIELD city ON record PERMISSIONS FOR select WHERE age < 200;
	DEFINE FIELD score ON record PERMISSIONS FOR select WHERE age < 200;
	DEFINE FIELD status ON record PERMISSIONS FOR select WHERE age < 200;
	DEFINE FIELD tier ON record PERMISSIONS FOR select WHERE age < 200;
	DEFINE FIELD http_status ON record PERMISSIONS FOR select WHERE age < 200;
	DEFINE FIELD created_at ON record PERMISSIONS FOR select WHERE age < 200;

	RETURN 'OK';
}
