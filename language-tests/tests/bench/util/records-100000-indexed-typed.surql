/**
[env]
imports = ["./records-seq-100000.surql"]

[test]
run = false
reason = "The sequential 100k records with `number` declared an int and indexed beside `status`, so an index over `number` holds one entry per record."

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD number ON record TYPE int;
	DEFINE INDEX number ON record FIELDS number;
	DEFINE INDEX status ON record FIELDS status;
	RETURN 'OK';
}
