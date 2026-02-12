/**
[env]
imports = ["./create_existing_record_field_owner.surql"]

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

SELECT * FROM full:empty;
SELECT * FROM `none`:empty;
SELECT * FROM `select`:empty;
SELECT * FROM `update`:empty;
SELECT * FROM `create`:empty;
