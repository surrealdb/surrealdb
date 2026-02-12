/**
[env]
imports = ["./basic_import.surql"]
auth = { level = "owner" }

[test]

[[test.results]]
value = "[{ create: 2, full: 2, id: full:filled, none: 2, select: 2, update: 2 }]"

[[test.results]]
value = "[{ create: 2, full: 2, id: `none`:filled, none: 2, select: 2, update: 2 }]"

[[test.results]]
value = "[{ create: 2, full: 2, id: select:filled, none: 2, select: 2, update: 2 }]"

[[test.results]]
value = "[{ create: 2, full: 2, id: update:filled, none: 2, select: 2, update: 2 }]"

[[test.results]]
value = "[{ create: 2, full: 2, id: create:filled, none: 2, select: 2, update: 2 }]"
*/

UPDATE full:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `none`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `select`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `update`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `create`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
