/**
# `records-10000-perms-full` plus a single field-level SELECT permission.
#
# The guard (`age < 200`) is satisfied by every row (`age` is 1..100), so this
# arm returns exactly the same documents as the unguarded `perms-full` arm it
# imports — the delta is the field-permission machinery alone: the per-row
# `(idiom, permission)` walk, the `Value::each` expansion of each guarded path,
# and the loss of the raw-byte read paths that require every referenced root
# field to carry an `Allow` SELECT permission (the pre-decode WHERE filter and
# the TopK threshold probe both refuse a guarded field).
#
# `number` is the guarded field on purpose: it is an integer, which is the only
# type the pre-decode filter can compare at wire level, and it is indexed in the
# `-indexed-` variants. `run = false`.

[env]
imports = ["./records-10000-perms-full.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD number ON record PERMISSIONS FOR select WHERE age < 200;

	RETURN 'OK';
}
