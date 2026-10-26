/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL string_agg, array_agg and json_agg over rows"

[[test.results]]
value = "[{ a: 'a,b,c', b: [1, 2, 3], c: [1, 2, 3] }]"
*/

SELECT string_agg(s, ',') AS a, array_agg(n) AS b, json_agg(n) AS c FROM (SELECT n, s FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(n, s) ORDER BY n) AS q;
