/**
[env]
imports = ["./create_existing_record_field_viewer.surql"]

[test]

[[test.results]]
value = "[{ full: 1, id: full:empty, update: 1 }]"

[[test.results]]
value = "[{ id: `none`:empty }]"

[[test.results]]
value = "[{ id: select:empty }]"

[[test.results]]
value = "[{ full: 1, id: update:empty, update: 1 }]"

[[test.results]]
value = "[{ id: create:empty }]"

*/

SELECT * FROM full:empty;
SELECT * FROM `none`:empty;
SELECT * FROM `select`:empty;
SELECT * FROM `update`:empty;
SELECT * FROM `create`:empty;
