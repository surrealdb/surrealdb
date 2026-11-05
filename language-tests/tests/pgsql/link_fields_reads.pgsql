/**
[env]
namespace = true
database = true
imports = ["pgsql/_link_fields.surql"]

[test]
reason = "A column declared as a link to one table is compared with a key as it is with the link: an integer or string key, IN, <>, a subquery and a join on the id of the linked table find the rows that hold the link, and a key that names no record, NULL or a float matches nothing and does not stop the statement. A column the table does not declare is compared as it is."

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Bob' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Sam' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Sam' }, { name: 'Bob' }]"

[[test.results]]
value = "[{ name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Ann' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 1 }]"

[[test.results]]
value = "[{ dname: 'Engineering', name: 'Eve' }, { dname: 'Sales', name: 'Sam' }, { dname: 'Engineering', name: 'Ann' }, { dname: 'Operations', name: 'Bob' }]"

[[test.results]]
value = "[{ name: 'Sam' }]"

[[test.results]]
value = "[{ mentor: 'Eve', name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Ann' }]"

[[test.results]]
value = "[{ name: 'Sam' }]"

[[test.results]]
value = "[{ name: 'Ann' }]"

[[test.results]]
value = "[]"

*/

SELECT name FROM emp WHERE dept = 1 ORDER BY id;

SELECT name FROM emp WHERE dept = 'ops';

SELECT name FROM emp WHERE dept IN (1, 2) ORDER BY id;

SELECT name FROM emp WHERE dept <> 1 ORDER BY id;

SELECT name FROM emp WHERE dept = 1 AND mentor = 1;

SELECT name FROM emp WHERE mentor = 1;

SELECT name FROM emp WHERE dept = 1.5;

SELECT name FROM emp WHERE dept = NULL;

SELECT COUNT(*) AS n FROM emp WHERE dept = 2;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.id ORDER BY e.id;

SELECT e.name FROM emp e JOIN dept d ON d.id = e.dept WHERE d.dname = 'Sales';

SELECT e.name, m.name AS mentor FROM emp e JOIN emp m ON e.mentor = m.id ORDER BY e.id;

SELECT name FROM emp WHERE dept IN (SELECT id FROM dept WHERE dname LIKE 'E%') ORDER BY id;

SELECT name FROM emp WHERE dept = (SELECT id FROM dept WHERE dname = 'Sales');

SELECT name FROM emp WHERE dept = mentor;

SELECT ref FROM plain WHERE ref = 1;
