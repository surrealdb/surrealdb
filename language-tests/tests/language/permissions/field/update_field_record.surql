/**
[env]
imports = ["./basic_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:test" }

[test]

[[test.results]]
value = "[{ full: 2, id: full:filled, select: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

UPDATE full:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `none`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `select`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `update`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
UPDATE `create`:filled SET full = 2, `none` = 2, `select` = 2, `create` = 2, `update` = 2;
