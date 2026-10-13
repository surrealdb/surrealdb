/**
[env]
imports = ["language/closure/matrix/perm/sel_cmp_let_hidden_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR select WHERE n >= $m`, read by a record user. A `LET` of the statement that defines or runs a stored clause is not visible in the clause's own frame, so the clause reads the name as unbound."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 10]"
*/
LET $m = 8;
SELECT VALUE n FROM pt_cmp_let_hidden ORDER BY n;
