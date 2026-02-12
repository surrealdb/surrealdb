/**
[env]
imports = ["./basic_import.surql"]
auth = { level = "viewer" }

[test]

[[test.results]]
value = "[{ create: 1, full: 2, id: full:filled, none: 1, select: 1, update: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ create: 1, full: 2, id: update:filled, none: 1, select: 1, update: 2 }]"

[[test.results]]
value = "[]"

*/

UPDATE full:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `none`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `select`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `update`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `create`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
