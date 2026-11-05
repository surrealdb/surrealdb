/**
[env]
imports = ["./create-range-schema.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE INDEX by_n ON record FIELDS n;
	RETURN 'OK';
}
