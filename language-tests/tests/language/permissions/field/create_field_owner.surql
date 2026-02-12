/**
[env]
imports = ["./basic_import.surql"]
auth = { level = "owner" }

[test]

[[test.results]]
value = "[{ create: 1, full: 1, id: full:new, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: `none`:new, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: select:new, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: update:new, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: create:new, none: 1, select: 1, update: 1 }]"

*/

CREATE full:new SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE `none`:new SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE `select`:new SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE `update`:new SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
CREATE `create`:new SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
