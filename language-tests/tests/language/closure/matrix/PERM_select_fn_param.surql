/**
[env]
imports = ["language/closure/matrix/perm/sel_fn_param_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR select WHERE fn::ok(n, $pm)`, read by a record user. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[10]"
*/
SELECT VALUE n FROM pt_fn_param ORDER BY n;
