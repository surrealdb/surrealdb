/**
[env]
imports = ["./records-seq-100000.surql"]

[test]
run = false
reason = "The sequential 100k records with `integer` declared an int under a UNIQUE index, so the key at either end of the index is held by one row."

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD integer ON record TYPE int;
	DEFINE INDEX integer_unique ON record FIELDS integer UNIQUE;
	RETURN 'OK';
}
