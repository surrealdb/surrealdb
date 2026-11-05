/**
[env]
namespace = true
database = true
imports = ["pgsql/_native.surql"]

[test]
reason = "Joins whose left input yields at most one row — a derived table with LIMIT 1, or an aggregate over every row — hold that row and stream the right input past it; each pairs the rows a hash join over the right input pairs, in its order, for an integer, NULL and missing key, a LEFT JOIN nothing matches, an empty left input and a LIMIT above the join"

[[test.results]]
value = "[{ m: 120.5f, name: 'Cara' }]"

[[test.results]]
value = "[{ dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }]"

[[test.results]]
value = "[{ dname: 'Ops', name: 'Cara' }]"

[[test.results]]
value = "[{ dname: NONE, name: 'Hana' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Ops2', name: 'Cara' }]"

[[test.results]]
value = "[]"
*/

SELECT e.name, s.m FROM (SELECT max(salary) AS m FROM emp) s JOIN emp e ON e.salary = s.m;

SELECT s.name, d.dname FROM (SELECT e.name AS name, e.dept AS dept FROM emp e ORDER BY e.salary DESC LIMIT 1) s JOIN dept d ON s.dept = d.did;

SELECT s.name, d.dname FROM (SELECT e.name AS name, e.dept AS dept FROM emp e ORDER BY e.salary DESC LIMIT 1) s JOIN dept d ON s.dept = d.did LIMIT 1;

SELECT s.name, d.dname FROM (SELECT e.name AS name, e.dept AS dept FROM emp e WHERE e.name = 'Hana' LIMIT 1) s LEFT JOIN dept d ON s.dept = d.did;

SELECT s.name, d.dname FROM (SELECT e.name AS name, e.dept AS dept FROM emp e WHERE e.name = 'Finn' LIMIT 1) s JOIN dept d ON s.dept = d.did;

SELECT s.name, d.dname FROM (SELECT e.name AS name, e.dept AS dept FROM emp e WHERE e.name = 'Gus' LIMIT 1) s JOIN dept d ON s.dept = d.did;

SELECT s.name, d.dname FROM (SELECT e.name AS name, e.dept AS dept FROM emp e WHERE e.name = 'Cara' LIMIT 1) s JOIN dept d ON s.dept = d.did AND d.floor > 1;

SELECT s.name, d.dname FROM (SELECT e.name AS name, e.dept AS dept FROM emp e WHERE e.name = 'Nobody' LIMIT 1) s LEFT JOIN dept d ON s.dept = d.did;
