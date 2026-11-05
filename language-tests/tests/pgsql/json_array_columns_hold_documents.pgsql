/**
[env]
namespace = true
database = true
imports = ["pgsql/_json_array_columns.surql"]

[test]
reason = "A json[] or jsonb[] column holds JSON documents as its elements, whatever keys they carry: a nullable column, a NOT NULL one and a two-dimensional one each keep the objects written into them in the SCHEMAFULL table CREATE TABLE defines"

[[test.results]]
value = "[{ docs: [{ a: 1 }, { b: { c: [2, { d: NULL }] } }], id: 1 }]"

[[test.results]]
value = "[{ docs: [{ z: { y: 1 } }, { a: 1 }], id: 1 }]"

[[test.results]]
value = "[{ docs: [[{ a: 1 }], [{ g: { h: true } }]], id: 1 }]"

*/

SELECT id, docs FROM ja_nullable;
SELECT id, docs FROM ja_required;
SELECT id, docs FROM ja_grid;
