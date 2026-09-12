/**
[env]
imports = ["reproductions/permission_checked_param_import.surql"]
clean = true

signup = """{
	ns: "test",
	db: "test",
	ac: "account",
	email: "user@example.com",
}"""

[test]
reason = "Reading a DEFINE PARAM the caller may not view is denied even where no row survives the WHERE: the read is judged where it happens, so no rewrite may drop it"

[[test.results]]
error = "You don't have permission to view the $secret parameter"

[[test.results]]
error = "You don't have permission to view the $secret parameter"

[[test.results]]
value = "[]"

[[test.results]]
value = "[7]"
*/
-- The predicate admits no row, but the source still reads $secret.
SELECT * FROM [$secret] WHERE false;
-- The same read with nothing to drop it.
SELECT * FROM [$secret];
-- A parameter the caller may read is a value the statement holds before it
-- runs, so the source goes with the filter.
SELECT * FROM [$open] WHERE false;
SELECT * FROM [$open];
