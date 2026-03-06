/**
[env]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }
imports = ["language/permissions/dml/dml_permissions_setup.surql"]

[test]
reason = "Record user: RELATE respects table CREATE permissions on the relation table"

[[test.results]]
value = "[]"

[[test.results]]
match = "array::len($result) == 1 && $result[0].in == user:1 && $result[0].out == user:2"

[[test.results]]
match = "array::len($result) == 1 && $result[0].in == user:1 && $result[0].out == user:2"

[[test.results]]
value = "[]"

*/
-- rel: create NONE -> denied
RELATE user:1->rel->user:2;
-- rel_full: create FULL -> allowed
RELATE user:1->rel_full->user:2;
-- rel_where: create WHERE in = $auth -> allowed (in = user:1 = $auth)
RELATE user:1->rel_where->user:2;
-- rel_where: create WHERE in = $auth -> denied (in = user:2 != $auth)
RELATE user:2->rel_where->user:1;
