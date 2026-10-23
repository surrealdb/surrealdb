/**
[env]
namespace = true
database = true
imports = ["pgsql/_aggregate_nulls.surql", "pgsql/_collection_values.surql"]

[test]
reason = "string_agg joins the non-NULL values of a group as text, array_agg and json_agg collect them with their NULLs, and all are NULL over no row"

[[test.results]]
value = "[{ names: 'ada,bob,cy,dan' }]"

[[test.results]]
value = "[{ grp: 'x', names: 'ada|bob' }, { grp: 'y', names: 'cy' }, { grp: 'z', names: 'dan' }]"

[[test.results]]
value = "[{ ages: [36, NULL] }]"

[[test.results]]
value = "[{ names: NULL }]"

[[test.results]]
value = "[{ grp: 'x', aged: 'ada' }, { grp: 'y', aged: 'cy' }, { grp: 'z', aged: NULL }]"

[[test.results]]
value = "[{ exact: '1.25,2.5,3', flags: 'true,false,true' }]"

[[test.results]]
value = "[{ names: 'adabobcydan' }]"

[[test.results]]
value = "[{ a: NULL, j: NULL, jb: NULL, s: NULL }]"

[[test.results]]
value = "[{ a: ['ada', 'bob', 'cy', 'dan'], j: ['ada', 'bob', 'cy', 'dan'], jb: ['ada', 'bob', 'cy', 'dan'] }]"
*/

SELECT string_agg(name, ',') AS names FROM agg_t;
SELECT grp, string_agg(name, '|') AS names FROM agg_t GROUP BY grp ORDER BY grp;
SELECT array_agg(age) AS ages FROM agg_t WHERE grp = 'x';
SELECT string_agg(name, ',') AS names FROM agg_t WHERE k > 100;
SELECT grp, string_agg(CASE WHEN age IS NULL THEN NULL ELSE name END, ',') AS aged FROM agg_t GROUP BY grp ORDER BY grp;
SELECT string_agg(exact, ',') AS exact, string_agg(flag, ',') AS flags FROM vals;
SELECT string_agg(label, NULL) AS names FROM vals;
SELECT array_agg(label) AS a, json_agg(label) AS j, jsonb_agg(label) AS jb, string_agg(label, ',') AS s FROM vals WHERE k > 100;
SELECT array_agg(label) AS a, json_agg(label) AS j, jsonb_agg(label) AS jb FROM vals;
