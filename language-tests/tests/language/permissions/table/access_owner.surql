/**
[env]
imports = ["language/permissions/table/basic_import.surql"]
auth = { level = "owner" }

[test]

[[test.results]]
value = "{ create: 'DEFINE TABLE `create` TYPE ANY SCHEMALESS PERMISSIONS FOR select, update, delete NONE, FOR create FULL', delete: 'DEFINE TABLE delete TYPE ANY SCHEMALESS PERMISSIONS FOR select, create, update NONE', delete_select: 'DEFINE TABLE delete_select TYPE ANY SCHEMALESS PERMISSIONS FOR select FULL, FOR create, update NONE', full: 'DEFINE TABLE full TYPE ANY SCHEMALESS PERMISSIONS FULL', none: 'DEFINE TABLE `none` TYPE ANY SCHEMALESS PERMISSIONS NONE', select: 'DEFINE TABLE `select` TYPE ANY SCHEMALESS PERMISSIONS FOR select FULL, FOR create, update, delete NONE', update: 'DEFINE TABLE `update` TYPE ANY SCHEMALESS PERMISSIONS FOR select, create, delete NONE, FOR update FULL' }"

[[test.results]]
value = "'CREATE'"

[[test.results]]
value = "[{ id: `none`:2 }]"

[[test.results]]
value = "[{ id: full:2 }]"

[[test.results]]
value = "[{ id: select:2 }]"

[[test.results]]
value = "[{ id: create:2 }]"

[[test.results]]
value = "[{ id: update:2 }]"

[[test.results]]
value = "[{ id: delete:2 }]"

[[test.results]]
value = "[{ id: delete_select:2 }]"

[[test.results]]
value = "'UPDATE'"

[[test.results]]
value = "[{ id: `none`:1, v: 1 }]"

[[test.results]]
value = "[{ id: full:1, v: 1 }]"

[[test.results]]
value = "[{ id: select:1, v: 1 }]"

[[test.results]]
value = "[{ id: create:1, v: 1 }]"

[[test.results]]
value = "[{ id: update:1, v: 1 }]"

[[test.results]]
value = "[{ id: delete:1, v: 1 }]"

[[test.results]]
value = "[{ id: delete_select:1, v: 1 }]"

[[test.results]]
value = "'SELECT'"

[[test.results]]
value = "[{ id: `none`:1, v: 1 }, { id: `none`:2 }]"

[[test.results]]
value = "[{ id: full:1, v: 1 }, { id: full:2 }]"

[[test.results]]
value = "[{ id: select:1, v: 1 }, { id: select:2 }]"

[[test.results]]
value = "[{ id: create:1, v: 1 }, { id: create:2 }]"

[[test.results]]
value = "[{ id: update:1, v: 1 }, { id: update:2 }]"

[[test.results]]
value = "[{ id: delete:1, v: 1 }, { id: delete:2 }]"

[[test.results]]
value = "[{ id: delete_select:1, v: 1 }, { id: delete_select:2 }]"

[[test.results]]
value = "'DELETE'"

[[test.results]]
value = "[{ id: `none`:1, v: 1 }]"

[[test.results]]
value = "[{ id: full:1, v: 1 }]"

[[test.results]]
value = "[{ id: select:1, v: 1 }]"

[[test.results]]
value = "[{ id: create:1, v: 1 }]"

[[test.results]]
value = "[{ id: update:1, v: 1 }]"

[[test.results]]
value = "[{ id: delete:1, v: 1 }]"

[[test.results]]
value = "[{ id: delete_select:1, v: 1 }]"
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
