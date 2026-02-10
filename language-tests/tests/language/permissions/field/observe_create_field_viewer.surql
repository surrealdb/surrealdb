/**
[env]
imports = ["./create_field_viewer.surql"]

[test]

[[test.results]]
value = "[{ create: 1, full: 1, id: full:new }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ create: 1, full: 1, id: create:new }]"

*/

SELECT * FROM full:new;
SELECT * FROM `none`:new;
SELECT * FROM `select`:new;
SELECT * FROM `update`:new;
SELECT * FROM `create`:new;
