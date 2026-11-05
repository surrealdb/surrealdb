/**
[env]
namespace = true
database = true
imports = ["pgsql/_cast_nulls.surql"]

[test]
reason = "Casting NULL, a literal or a column, gives NULL whatever the target type; a text 'NULL' or '' is not NULL"

[[test.results]]
value = "[{ i: NULL, t: NULL }]"

[[test.results]]
value = "[{ f: 36f, i: 36, n: 36bigdec, t: '36' }, { f: NULL, i: NULL, n: NULL, t: NULL }, { f: 29f, i: 29, n: 29bigdec, t: '29' }, { f: NULL, i: NULL, n: NULL, t: NULL }, { f: NULL, i: NULL, n: NULL, t: NULL }]"

[[test.results]]
value = "[{ i: 36 }, { i: 29 }]"

[[test.results]]
value = "[{ i: 36 }, { i: NULL }, { i: 29 }, { i: NULL }, { i: NULL }]"

[[test.results]]
value = "[{ k: 2 }, { k: 4 }, { k: 5 }]"

[[test.results]]
value = "[{ k: 3 }, { k: 1 }, { k: 2 }, { k: 4 }, { k: 5 }]"

[[test.results]]
value = "[{ a: 'NULL', b: '', c: 'ada' }]"
*/

SELECT CAST(NULL AS INT) AS i, CAST(NULL AS TEXT) AS t;
SELECT CAST(age AS INT) AS i, CAST(age AS TEXT) AS t, CAST(age AS FLOAT) AS f, CAST(age AS NUMERIC) AS n FROM agg_t ORDER BY k;
SELECT age::int AS i FROM agg_t WHERE age IS NOT NULL ORDER BY k;
SELECT age::int::text::int AS i FROM agg_t ORDER BY k;
SELECT k FROM agg_t WHERE age::int IS NULL ORDER BY k;
SELECT k FROM agg_t ORDER BY age::int, k;
SELECT 'NULL'::text AS a, ''::text AS b, name::text AS c FROM agg_t WHERE k = 1;
