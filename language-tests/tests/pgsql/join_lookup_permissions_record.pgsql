/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup_permissions.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:alice" }

[test]
reason = "A join of a record user whose right table is indexed on the key, hides rows behind a table permission and computes a field: the permission and the computed field judge every row the lookup reads, for as few left rows as the lookup serves and as many as the build table takes over."

[[test.results]]
value = "[{ n: 1, up: 'T1' }, { n: 2, up: 'T2' }, { n: 4, up: 'T4' }, { n: 5, up: 'T5' }]"

[[test.results]]
value = "[{ n: 0, up: NONE }, { n: 1, up: 'T1' }, { n: 2, up: 'T2' }, { n: 3, up: NONE }, { n: 4, up: 'T4' }, { n: 5, up: 'T5' }, { n: 6, up: NONE }]"

[[test.results]]
value = "[{ c: 33 }]"

[[test.results]]
value = "[{ c: 120, named: 80 }]"

[[test.results]]
value = "[{ n: 65 }]"
*/

SELECT l.n, r.up FROM (SELECT n, j FROM a ORDER BY n LIMIT 7) l JOIN p r ON l.j = r.j ORDER BY l.n;

SELECT l.n, r.up FROM (SELECT n, j FROM a ORDER BY n LIMIT 7) l LEFT JOIN p r ON l.j = r.j ORDER BY l.n;

SELECT count(*) AS c FROM (SELECT n, j FROM a ORDER BY n LIMIT 50) l JOIN p r ON l.j = r.j;

SELECT count(*) AS c, count(r.up) AS named FROM (SELECT n, j FROM a ORDER BY n LIMIT 120) l LEFT JOIN p r ON l.j = r.j;

SELECT l.n FROM (SELECT n, j FROM a ORDER BY n LIMIT 70) l JOIN p r ON l.j = r.j AND r.up = 'T65' ORDER BY l.n;
