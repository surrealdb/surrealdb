/**
[env]
imports = ["language/permissions/table/basic_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:test" }

[test]

[[test.results]]
error = "IAM error: Not enough permissions to perform this action"

[[test.results]]
value = "'CREATE'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: full:2 }]"

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
value = "'UPDATE'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: full:1, v: 1 }]"

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
value = "'SELECT'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: full:1, v: 1 }, { id: full:2 }]"

[[test.results]]
value = "[{ id: select:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: delete_select:1 }]"

[[test.results]]
value = "'DELETE'"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: full:1, v: 1 }]"

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

(INFO FOR DB).tables;

"CREATE";

CREATE `none`:2;
CREATE `full`:2;
CREATE `select`:2;
CREATE `create`:2;
CREATE `update`:2;
CREATE `delete`:2;
CREATE `delete_select`:2;

"UPDATE";

UPDATE `none`:1 SET v = 1;
UPDATE `full`:1 SET v = 1;
UPDATE `select`:1 SET v = 1;
UPDATE `create`:1 SET v = 1;
UPDATE `update`:1 SET v = 1;
UPDATE `delete`:1 SET v = 1;
UPDATE `delete_select`:1 SET v = 1;

"SELECT";

SELECT * FROM `none`;
SELECT * FROM `full`;
SELECT * FROM `select`;
SELECT * FROM `create`;
SELECT * FROM `update`;
SELECT * FROM `delete`;
SELECT * FROM `delete_select`;

"DELETE";

DELETE `none`:1 RETURN BEFORE;
DELETE `full`:1 RETURN BEFORE;
DELETE `select`:1 RETURN BEFORE;
DELETE `create`:1 RETURN BEFORE;
DELETE `update`:1 RETURN BEFORE;
DELETE `delete`:1 RETURN BEFORE;
DELETE `delete_select`:1 RETURN BEFORE;
