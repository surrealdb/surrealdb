/**
[env]
imports = ["language/closure/matrix/perm/sel_cl_auth_id_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR select WHERE array::any([$auth.id], |$e| $e = person:bob)`, read by a record user. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[1, 10]"
*/
SELECT VALUE n FROM pt_cl_auth_id ORDER BY n;
