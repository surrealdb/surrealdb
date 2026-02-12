/**
[env]
imports = ["./basic_import.surql"]
auth = { level = "owner" }

[test]

[[test.results]]
value = "[{ create: 1, full: 1, id: full:empty, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: `none`:empty, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: select:empty, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: update:empty, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: create:empty, none: 1, select: 1, update: 1 }]"

*/

UPDATE full:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `none`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `select`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `update`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `create`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
