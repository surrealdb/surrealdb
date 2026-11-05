/**
# `records-10000-computed` read as a record user with table permissions FULL,
# plus a fourth `COMPUTED` field whose body reads two stored fields through
# `??` so it stays defined when one of them is withheld.
#
# Imports the computed 10k-row dataset and the `perms-full` scaffolding (RECORD
# access method, `user:bench`, table permissions FULL); the shared
# `records-10000` base is imported once. No field on the table restricts reads,
# so a read pays computed-field materialisation but none of the field-permission
# machinery — this is the baseline arm the guarded and opaque-dependency
# variants are measured against:
#
#  - `records-10000-computed-guarded-open` / `-cut` add a guard on `status`,
#  - `records-10000-computed-deps-static` / `-opaque` add a guard on `name`.
#
# `status_tag` is deliberately untyped and coalescing: when a guard withholds
# `status`, the field is re-derived against the reduced record and its body
# reads `NONE` there, which a declared non-optional type would reject.
# `run = false`.

[env]
imports = ["./records-10000-computed.surql", "./records-10000-perms-full.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD status_tag ON record
		COMPUTED string::concat(city ?? 'unknown', ' / ', status ?? 'unknown');

	RETURN 'OK';
}
