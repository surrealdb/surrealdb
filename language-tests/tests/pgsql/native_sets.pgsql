/**
[env]
namespace = true
database = true
imports = ["pgsql/_native.surql"]

[test]
reason = "PostgreSQL set operations and statements in FROM"

[[test.results]]
value = "[{ k: 1 }, { k: 2 }, { k: 3 }, { k: 4 }, { k: 9 }, { k: NONE }, { k: NULL }]"

[[test.results]]
value = "[{ k: 1 }, { k: 1 }, { k: 1 }, { k: 2 }, { k: 2 }, { k: 2 }, { k: 2 }, { k: 3 }, { k: 4 }, { k: 9 }, { k: NONE }, { k: NONE }, { k: NULL }, { k: NULL }]"

[[test.results]]
value = "[{ k: 1 }, { k: 1 }, { k: 2 }, { k: 2 }, { k: 3 }, { k: NULL }, { k: NONE }, { k: 9 }, { k: 1 }, { k: 2 }, { k: 2 }, { k: 4 }, { k: NULL }, { k: NONE }]"

[[test.results]]
value = "[{ k: 1 }, { k: 2 }, { k: 3 }, { k: NULL }, { k: NONE }, { k: 9 }, { k: 4 }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Eng' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Eng' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Alice' }, { n: 'Bob' }]"

[[test.results]]
value = "[{ c: 'London' }, { c: 'Paris' }, { c: 'Berlin' }, { c: 'Rome' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Cara' }, { n: 'Dan' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Cara' }, { n: 'Dan' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Eng' }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Cara' }, { name: 'Hana' }]"

[[test.results]]
value = "[{ name: 'Cara', salary: 120.5f }, { name: 'Hana', salary: 110 }, { name: 'Alice', salary: 100 }]"

[[test.results]]
value = "[{ n: 2 }]"

[[test.results]]
value = "[{ city: 'Berlin', n: 2 }, { city: 'London', n: 3 }, { city: 'Paris', n: 2 }]"

[[test.results]]
value = "[{ city: 'Berlin' }, { city: 'London' }, { city: 'Paris' }, { city: 'Rome' }]"
*/

SELECT e.dept AS k FROM emp e UNION SELECT d.did AS k FROM dept d ORDER BY k;

SELECT e.dept AS k FROM emp e UNION ALL SELECT d.did AS k FROM dept d ORDER BY k;

SELECT e.dept AS k FROM emp e UNION ALL SELECT d.did AS k FROM dept d;

SELECT e.dept AS k FROM emp e UNION SELECT d.did AS k FROM dept d;

SELECT e.name AS n FROM emp e WHERE e.dept = 1 UNION SELECT d.dname AS n FROM dept d WHERE d.did = 1;

SELECT e.name AS n FROM emp e WHERE e.dept = 1 UNION ALL SELECT d.dname AS n FROM dept d WHERE d.did = 1;

SELECT e.name AS n FROM emp e WHERE e.dept = 1 UNION SELECT e2.name AS n FROM emp e2 WHERE e2.dept = 1;

SELECT e.name AS n FROM emp e WHERE e.dept = 1 UNION ALL SELECT e2.name AS n FROM emp e2 WHERE e2.dept = 1;

SELECT e.city AS c FROM emp e UNION SELECT e.city AS c FROM emp e UNION SELECT e.city AS c FROM emp e;

SELECT e.name AS n FROM emp e WHERE e.dept = 1 UNION ALL SELECT x.id AS n FROM empty x;

SELECT x.id AS n FROM empty x UNION SELECT x.id AS n FROM empty x;

SELECT e.name AS n FROM emp e UNION SELECT d.dname AS n FROM dept d ORDER BY n LIMIT 4;

SELECT e.name AS n FROM emp e WHERE e.dept = 1 UNION SELECT e.name AS n FROM emp e WHERE e.dept = 2;

SELECT u.n FROM (SELECT e.name AS n FROM emp e WHERE e.dept = 1 UNION ALL SELECT d.dname AS n FROM dept d WHERE d.did = 1) u ORDER BY u.n;

SELECT s.name FROM (SELECT e.name AS name FROM emp e WHERE e.salary > 90) s ORDER BY s.name;

SELECT s.name, s.salary FROM (SELECT e.name AS name, e.salary AS salary FROM emp e) s WHERE s.salary > 90 ORDER BY s.salary DESC;

SELECT count(*) AS n FROM (SELECT e.name AS name FROM emp e WHERE e.dept = 1) s;

SELECT s.city, s.n FROM (SELECT e.city AS city, count(*) AS n FROM emp e GROUP BY e.city) s WHERE s.n > 1 ORDER BY s.city;

SELECT DISTINCT s.city FROM (SELECT e.city AS city FROM emp e) s ORDER BY s.city;
