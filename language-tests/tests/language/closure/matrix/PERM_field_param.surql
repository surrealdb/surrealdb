/**
[env]
imports = ["language/closure/matrix/perm/fld_param_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a field `PERMISSIONS FOR select WHERE array::any([$value], |$e| $e >= $pm)`, run by a record user. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[NONE, 10]"
*/
SELECT VALUE f FROM pt_fld ORDER BY id;
