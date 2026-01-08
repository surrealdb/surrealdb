/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: product:test }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: { }, fields: { id_copied: 'DEFINE FIELD id_copied ON product TYPE record DEFAULT id PERMISSIONS FULL' }, indexes: { }, lives: { }, tables: { } }"

[[test.results]]
value = "{ events: [ ], fields: [ { default: 'id', default_always: false, kind: 'record', name: 'id_copied', permissions: { create: true, select: true, update: true }, readonly: false, table: 'product' } ], indexes: [ ], lives: [ ], tables: [ ] }"

[[test.results]]
value = "[{ id: product:a, id_copied: product:a }]"

[[test.results]]
value = "[{ id: product:b, id_copied: product:b }]"

*/

DEFINE TABLE product SCHEMAFULL;
--
CREATE product:test;

DEFINE FIELD id_copied ON product TYPE record DEFAULT id;
--
INFO FOR TABLE product;
INFO FOR TABLE product STRUCTURE;
--
CREATE product:a;
UPSERT product:b;
