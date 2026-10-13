/**
[env]
imports = ["language/closure/matrix/perm/sel_cl_rowfield_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR select WHERE array::any([n], |$e| $e >= lim)`, read by a record user. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[10]"
*/
SELECT VALUE n FROM pt_cl_rowfield ORDER BY n;
