/**
[env]
imports = ["language/permissions/field/basic_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:test" }

*/

INFO FOR TABLE t;
SELECT * FROM t;
UPDATE t:1 SET full = 2, none = 2, select = 2, create = 2, update = 2 RETURN AFTER;
UPDATE t:2 SET full = 2, none = 2, select = 2, create = 2, update = 2 RETURN AFTER;
