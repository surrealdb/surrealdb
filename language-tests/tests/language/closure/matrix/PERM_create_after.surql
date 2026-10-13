/**
[env]
imports = ["language/closure/matrix/perm/create_after_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR create WHERE array::any([$after.n], |$e| $e >= $pm)`, run by a record user. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/
CREATE pt_create:1 SET n = 1;
CREATE pt_create:2 SET n = 10;
SELECT VALUE n FROM pt_create ORDER BY n;
