/**
# `records-10000-computed-perms` plus a field-level SELECT permission on `name`
# whose expression reads only stored fields.
#
# Because the guard's dependencies are statically resolvable (`age`, nothing
# else), a selective projection can be answered without deriving the computed
# fields it does not name: the fields a read must materialise are the projected
# ones unioned with the ones the guard expressions reference. The guard holds
# for every row (`age` is 1..100), so this arm returns exactly the same
# documents as `records-10000-computed-deps-opaque`, where the same guard is
# written with a parameter and forces every computed field on the table to be
# derived whatever the projection asks for. `run = false`.

[env]
imports = ["./records-10000-computed-perms.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD name ON record PERMISSIONS FOR select WHERE age < 200;

	RETURN 'OK';
}
