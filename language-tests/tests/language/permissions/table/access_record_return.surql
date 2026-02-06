/**
[env]
imports = ["language/permissions/table/basic_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:test" }

[test]
reason = "Do not leak date from unselectable tables through return clauses"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: delete_select:1 }]"
*/

SEELCT * FROM `update`:1;
UPDATE `update`:1 SET v = 1 RETURN BEFORE;
UPDATE `update`:1 SET v = 2 RETURN AFTER;
UPDATE `update`:1 SET v = 3 RETURN $this;
UPDATE `update`:1 SET v = 4 RETURN $parent;
UPDATE `update`:1 SET v = 5 RETURN v;

CREATE `create`:2 RETURN AFTER;
DELETE `delete`:1 RETURN BEFORE;
DELETE `delete_select`:1 RETURN BEFORE;
