/**
[env]
imports = ["access/record/basic_access_import.surql"]
clean = true

signup = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "bla",
	pass: "hunter2",
}"""

[test]

[test.results]
signup-error = "An error occurred: Email field is not a valid email"

*/


1;
