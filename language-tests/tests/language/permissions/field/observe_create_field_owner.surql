/**
[env]
imports = ["./create_field_owner.surql"]

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

SELECT * FROM full:new;
SELECT * FROM `none`:new;
SELECT * FROM `select`:new;
SELECT * FROM `update`:new;
SELECT * FROM `create`:new;
