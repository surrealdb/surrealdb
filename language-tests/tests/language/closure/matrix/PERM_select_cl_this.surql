/**
[env]
imports = ["language/closure/matrix/perm/sel_cl_this_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR select WHERE array::any([1, 2], |$e| $e <= $this.n)`, read by a record user. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[1, 10]"
*/
SELECT VALUE n FROM pt_cl_this ORDER BY n;
