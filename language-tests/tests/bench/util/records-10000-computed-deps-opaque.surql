/**
# `records-10000-computed-perms` plus a field-level SELECT permission on `name`
# whose expression reads a parameter.
#
# A parameter binds to anything at runtime, so static dependency analysis of the
# guard cannot enumerate the fields it might read and reports its dependency set
# as incomplete. A read then cannot prove that a computed field is unnecessary,
# and derives every one of them per row — including for a projection that names
# none. `$auth` is the record the bench session authenticated as, so the guard
# holds for every row and this arm returns the same documents as
# `records-10000-computed-deps-static`; the delta is the derivation of four
# computed fields per row that the query never reads.
#
# The guard tests `$auth` itself rather than a field of it. `$auth` is a record
# id, so reading a field through it dereferences the record — and inside a
# permission clause that fetch skips permission checks, so it resolves rather
# than failing. Per row, per guarded field, that is a record fetch the statically
# analysable arm does not perform, and it would land in the delta this pair
# exists to attribute to computed-field derivation. `run = false`.

[env]
imports = ["./records-10000-computed-perms.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD name ON record PERMISSIONS FOR select WHERE $auth != NONE;

	RETURN 'OK';
}
