/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_json.surql"]

[test]
reason = "Text written to a json or jsonb column is JSON text: it is parsed into the value it spells, so the column holds an array and a scalar as well as an object, a jsonb column keeps the last of duplicate keys, and text that is not JSON is refused"

[[test.results]]
value = "[{ id: 1, j: [1, 2], jb: 5 }]"

[[test.results]]
value = "[{ id: 2, j: 'text', jb: { a: 2 } }]"

[[test.results]]
value = "[{ id: 3, j: { a: 2, b: 1 }, jb: [true, NULL] }]"

[[test.results]]
error = "Incorrect arguments for function encoding::json::decode(). Invalid JSON"

[[test.results]]
value = "[{ id: 1, j: [1, 2], jb: { x: [1, { y: NULL }] } }]"

[[test.results]]
value = "[{ id: 1, j: [1, 2], jb: { x: [1, { y: NULL }] } }, { id: 2, j: 'text', jb: { a: 2 } }, { id: 3, j: { a: 2, b: 1 }, jb: [true, NULL] }]"

[[test.results]]
value = "[{ a: '2', b: NULL }, { a: NULL, b: 1 }]"
*/

INSERT INTO jt (id, j, jb) VALUES (1, CAST('[1, 2]' AS json), CAST('5' AS jsonb));
INSERT INTO jt (id, j, jb) VALUES (2, CAST('"text"' AS json), CAST('{"a": 1, "a": 2}' AS jsonb));
INSERT INTO jt (id, j, jb) VALUES (3, '{"b": 1,  "a": 2}', '[true, null]');
INSERT INTO jt (id, j) VALUES (4, '{not json');
UPDATE jt SET jb = '{"x": [1, {"y": null}]}' WHERE id = 1;
SELECT id, j, jb FROM jt ORDER BY id;
SELECT j -> 'b' AS b, jb ->> 'a' AS a FROM jt WHERE id = 2 OR id = 3 ORDER BY id;
