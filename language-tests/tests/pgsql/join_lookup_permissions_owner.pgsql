/**
[env]
namespace = true
database = true
imports = ["pgsql/_join_lookup_permissions.surql"]
auth = { level = "owner" }

[test]
reason = "The joins of join_lookup_permissions_record.pgsql as the owner: every row of the right table is read and its computed field derived."

[[test.results]]
value = "[{ n: 0, up: 'T0' }, { n: 1, up: 'T1' }, { n: 2, up: 'T2' }, { n: 3, up: 'T3' }, { n: 4, up: 'T4' }, { n: 5, up: 'T5' }, { n: 6, up: 'T6' }]"

[[test.results]]
value = "[{ n: 0, up: 'T0' }, { n: 1, up: 'T1' }, { n: 2, up: 'T2' }, { n: 3, up: 'T3' }, { n: 4, up: 'T4' }, { n: 5, up: 'T5' }, { n: 6, up: 'T6' }]"

[[test.results]]
value = "[{ c: 50 }]"

[[test.results]]
value = "[{ c: 120, named: 120 }]"

[[test.results]]
value = "[{ n: 65 }]"
*/

SELECT l.n, r.up FROM (SELECT n, j FROM a ORDER BY n LIMIT 7) l JOIN p r ON l.j = r.j ORDER BY l.n;

SELECT l.n, r.up FROM (SELECT n, j FROM a ORDER BY n LIMIT 7) l LEFT JOIN p r ON l.j = r.j ORDER BY l.n;

SELECT count(*) AS c FROM (SELECT n, j FROM a ORDER BY n LIMIT 50) l JOIN p r ON l.j = r.j;

SELECT count(*) AS c, count(r.up) AS named FROM (SELECT n, j FROM a ORDER BY n LIMIT 120) l LEFT JOIN p r ON l.j = r.j;

SELECT l.n FROM (SELECT n, j FROM a ORDER BY n LIMIT 70) l JOIN p r ON l.j = r.j AND r.up = 'T65' ORDER BY l.n;
