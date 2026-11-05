/**
[env]
imports = ["language/closure/matrix/perm/update_before_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Position: a table `PERMISSIONS FOR update WHERE array::any([$before.n], |$e| $e >= $pm)`, run by a record user. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/
UPDATE pt_upd SET x = 1;
SELECT VALUE id FROM pt_upd WHERE x = 1 ORDER BY id;
