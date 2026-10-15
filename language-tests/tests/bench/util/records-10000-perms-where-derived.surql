/**
# `records-10000-perms-where` with a `COMPUTED` field the select permission reads.
#
# Imports the permissioned base, adds an untyped computed field derived from a
# stored one, and rewrites the table permissions so the select clause reads that
# field: a gate that judges the record has to derive the field onto a copy of the
# image first. Every `age` is below 200, so the rows a statement sees are those of
# the other permissioned datasets. The update and delete clauses keep reading the
# stored `age` alone. Paired with `records-10000-perms-full` and
# `records-10000-perms-where` in the write permission benches, where it is a
# control: a select clause over a `COMPUTED` field is one the stages cannot prove
# inert. `run = false`.

[env]
imports = ["./records-10000-perms-where.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD age_band ON record COMPUTED string::concat('b', <string> age);
	ALTER TABLE record PERMISSIONS
		FOR select WHERE age_band != 'x'
		FOR update, delete WHERE age < 200
		FOR create FULL;

	RETURN 'OK';
}
