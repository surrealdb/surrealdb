/**
[env]
imports = ["./basic_import.surql"]
auth = { level = "viewer" }

[test]

[[test.results]]
value = "[{ full: 1, id: full:empty, update: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ full: 1, id: update:empty, update: 1 }]"

[[test.results]]
value = "[]"

*/

UPDATE full:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `none`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `select`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `update`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `create`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
