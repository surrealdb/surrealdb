/**
[env]
imports = ["language/closure/matrix/perm/sel_cl_in_ef_param_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR select WHERE [n][WHERE array::any([$this], |$e| $e >= $pm)] != []`, read by a record user. It runs and reads the names and the row of the frame where the closure is written."

[[test.results]]
value = "[10]"
*/
SELECT VALUE n FROM pt_cl_in_ef_param ORDER BY n;
