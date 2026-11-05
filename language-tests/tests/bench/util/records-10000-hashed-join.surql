/**
[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD hidden ON record PERMISSIONS FOR select WHERE true;
	RETURN 'OK';
}
