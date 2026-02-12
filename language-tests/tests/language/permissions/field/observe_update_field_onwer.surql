/**
[env]
imports = ["./update_field_owner.surql"]

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

SELECT * FROM full:filled;
SELECT * FROM `none`:filled;
SELECT * FROM `select`:filled;
SELECT * FROM `update`:filled;
SELECT * FROM `create`:filled;
