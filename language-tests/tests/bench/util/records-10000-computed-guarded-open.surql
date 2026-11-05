/**
# `records-10000-computed-perms` plus a field-level SELECT permission on
# `status`, satisfied by every row (`age < 200`, and `age` is 1..100).
#
# Nothing is withheld, so this arm returns the same documents as the unguarded
# `records-10000-computed-perms` baseline. What it adds is the cost a table
# pays for having computed fields *and* a restricted field at once: the row is
# cloned per record so both the guard predicates and the re-derivation have an
# unreduced view to read, and every guard is expanded and judged. Paired with
# `records-10000-computed-guarded-cut`, where the same guard actually withholds
# the field, which is what triggers re-derivation on top of this. `run = false`.

[env]
imports = ["./records-10000-computed-perms.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD status ON record PERMISSIONS FOR select WHERE age < 200;

	RETURN 'OK';
}
