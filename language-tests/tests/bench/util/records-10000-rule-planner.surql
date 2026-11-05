/**
[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE INDEX by_age ON record FIELDS age;
	DEFINE INDEX by_created ON record FIELDS created_at;
	DEFINE INDEX by_number ON record FIELDS number;
	DEFINE TABLE person SCHEMALESS;
	CREATE |person:2000| SET n = rand::int(1, 5000);
	RETURN 'OK';
}
