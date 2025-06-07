/**
[env]
imports = ["access/record/basic_access_import.surql"]

signup = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "bla",
	pass: "hunter2",
}"""

[test]

[test.results]
signup-error = "Execution halted with output: `Email field is not a valid email`"

*/


1;
