/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL to_json, to_jsonb and json_build_object, whose keys may be any text"

[[test.results]]
value = "[{ a: 42, b: 1.5f, c: { a: 1 }, d: { x: 'y', z: NULL } }]"

[[test.results]]
value = "[{ n: 1, o: { k: 1 } }]"
*/

SELECT to_json(42) AS a, to_jsonb(1.5) AS b, json_build_object('a', 1) AS c, jsonb_build_object('x', 'y', 'z', NULL) AS d;

SELECT n, json_build_object(s, n) AS o FROM (SELECT 1 AS n, 'k' AS s) AS q;
