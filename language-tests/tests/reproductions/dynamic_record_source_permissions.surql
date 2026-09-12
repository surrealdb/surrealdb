/**
[env]
imports = ["reproductions/dynamic_record_source_import.surql"]
clean = true

signup = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "user@example.com",
}"""

[test]
reason = "The record a FROM expression's array names is read with the caller's SELECT permissions, so a record the caller may not read is no row of the source"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/
SELECT * FROM array::shuffle([closed:1]);
SELECT * FROM array::shuffle([closed:1]) WHERE x = 1;
SELECT * FROM fn::ids();
SELECT * FROM [closed:1];
