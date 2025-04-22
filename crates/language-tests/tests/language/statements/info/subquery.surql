/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ test: 'DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS NONE' }"

[[test.results]]
value = "[{ drop: false, full: true, kind: { kind: 'NORMAL' }, name: 'test', permissions: { create: false, delete: false, select: false, update: false } }]"

*/
DEFINE TABLE OVERWRITE test SCHEMAFULL;
(INFO FOR DB).tables;
(INFO FOR DB STRUCTURE).tables;
