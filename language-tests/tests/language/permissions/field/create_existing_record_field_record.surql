/**
[env]
imports = ["./basic_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:test" }

[test]

[[test.results]]
value = "[{ full: 1, id: full:empty }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

UPDATE full:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `none`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `select`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `update`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
UPDATE `create`:empty SET full = 1, `none` = 1, `select` = 1, `create` = 1, `update` = 1;
