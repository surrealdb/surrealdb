/**
[env]
imports = ["language/closure/matrix/perm/sel_subq_cl_param_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR select WHERE array::any((SELECT VALUE k FROM pt_perm_cl_param_k WHERE k >= $pm), |$e| $e <= n)`, read by a record user. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[]"
*/
SELECT VALUE n FROM pt_subq_cl_param ORDER BY n;
