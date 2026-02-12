/**
[env]
imports = ["./update_field_record.surql"]

[test]

[[test.results]]
value = "[{ create: 1, full: 2, id: full:filled, none: 1, select: 1, update: 2 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: `none`:filled, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: select:filled, none: 1, select: 1, update: 1 }]"

[[test.results]]
value = "[{ create: 1, full: 2, id: update:filled, none: 1, select: 1, update: 2 }]"

[[test.results]]
value = "[{ create: 1, full: 1, id: create:filled, none: 1, select: 1, update: 1 }]"
*/

SELECT * FROM full:filled;
SELECT * FROM `none`:filled;
SELECT * FROM `select`:filled;
SELECT * FROM `update`:filled;
SELECT * FROM `create`:filled;
