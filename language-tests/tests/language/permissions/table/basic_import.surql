/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: `none`:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: full:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: select:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: create:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: update:1, secret: 'hunter2' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: delete:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: delete_select:1 }]"

[[test.results]]
value = "NONE"

*/
DEFINE TABLE `none`;
CREATE `none`:1;

DEFINE TABLE full PERMISSIONS FULL;
CREATE full:1;

DEFINE TABLE `select` PERMISSIONS FOR select FULL;
CREATE `select`:1;

DEFINE TABLE `create` PERMISSIONS FOR create FULL;
CREATE `create`:1;

DEFINE TABLE `update` PERMISSIONS FOR update FULL;
CREATE `update`:1 SET secret = "hunter2";

DEFINE TABLE `delete` PERMISSIONS FOR delete FULL;
CREATE `delete`:1;

DEFINE TABLE delete_select PERMISSIONS FOR delete, select FULL;
CREATE delete_select:1;

DEFINE ACCESS test ON DATABASE;
