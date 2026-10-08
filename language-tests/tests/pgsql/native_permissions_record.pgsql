/**
[env]
namespace = true
database = true
imports = ["pgsql/_native_perms.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:alice" }

[test]
reason = "PostgreSQL reads as a record user: table permissions, field permissions and computed fields judged on the rows of each side of a join, a union and an aggregate"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }, { name: 'Cara' }, { name: 'Eve' }]"

[[test.results]]
value = "[{ name: 'Alice', salary: 100 }, { name: 'Bob', salary: 80 }, { name: 'Cara', salary: NONE }, { name: 'Eve', salary: 70 }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }]"

[[test.results]]
value = "[{ dname: 'Eng', label: 'ENG', name: 'Alice' }, { dname: 'Eng', label: 'ENG', name: 'Bob' }, { dname: 'Ops', label: 'OPS', name: 'Cara' }, { dname: NONE, label: NONE, name: 'Eve' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ hours: NONE, name: 'Alice' }, { hours: NONE, name: 'Bob' }, { hours: NONE, name: 'Cara' }, { hours: NONE, name: 'Eve' }]"

[[test.results]]
value = "[{ n: 4 }]"

[[test.results]]
value = "[{ dept: 1, n: 2, pay: 180 }, { dept: 2, n: 1, pay: 0 }, { dept: 3, n: 1, pay: 70 }]"

[[test.results]]
value = "[{ dept: 1, pay: 180 }]"

[[test.results]]
value = "[{ name: 'Alice' }]"

[[test.results]]
value = "[{ name: 'Eve' }, { name: 'Bob' }, { name: 'Alice' }, { name: 'Cara' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ name: 'Alice', v: 'one' }, { name: 'Bob', v: 'one' }, { name: 'Cara', v: 'two' }, { name: 'Eve', v: 'three' }]"

[[test.results]]
value = "[{ name: 'Alice', v: 'one' }, { name: 'Bob', v: 'one' }, { name: 'Eve', v: 'three' }, { name: 'Cara', v: 'two' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Cara' }, { n: 'Eng' }, { n: 'Eve' }, { n: 'Ops' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Cara' }, { n: 'Eve' }]"

[[test.results]]
value = "[{ dept: 1 }, { dept: 1 }, { dept: 2 }, { dept: 3 }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Cara' }, { name: 'Eve' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }]"

[[test.results]]
value = "[{ did: 1, dname: 'Eng', floor: 3, id: dept:1, label: 'ENG' }, { did: 2, dname: 'Ops', floor: 1, id: dept:2, label: 'OPS' }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2 }, { dname: 'Ops', n: 1 }]"
*/

SELECT e.name FROM emp e ORDER BY e.name;

SELECT e.name, e.salary FROM emp e ORDER BY e.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name, d.dname;

SELECT e.name, d.dname, d.label FROM emp e LEFT JOIN dept d ON e.dept = d.did ORDER BY e.name;

SELECT d.dname, e.name FROM dept d LEFT JOIN emp e ON e.dept = d.did ORDER BY d.dname, e.name;

SELECT e.name, p.hours FROM emp e JOIN proj p ON p.emp_id = e.id ORDER BY e.name;

SELECT e.name, p.hours FROM emp e LEFT JOIN proj p ON p.emp_id = e.id ORDER BY e.name;

SELECT count(*) AS n FROM emp e;

SELECT e.dept, count(*) AS n, sum(e.salary) AS pay FROM emp e GROUP BY e.dept ORDER BY e.dept;

SELECT e.dept, sum(e.salary) AS pay FROM emp e GROUP BY e.dept HAVING sum(e.salary) > 100 ORDER BY e.dept;

SELECT e.name FROM emp e WHERE e.salary > 90 ORDER BY e.name;

SELECT e.name FROM emp e ORDER BY e.salary, e.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.salary = d.floor * 20 ORDER BY e.name;

SELECT e.name, o.v FROM emp e JOIN open o ON e.dept = o.k ORDER BY e.name;

SELECT o.v, e.name FROM open o LEFT JOIN emp e ON e.dept = o.k ORDER BY o.v, e.name;

SELECT e.name AS n FROM emp e UNION SELECT d.dname AS n FROM dept d ORDER BY n;

SELECT e.name AS n FROM emp e UNION ALL SELECT p.hours AS n FROM proj p ORDER BY n;

SELECT DISTINCT e.dept FROM emp e ORDER BY e.dept;

SELECT s.name FROM (SELECT e.name AS name FROM emp e WHERE e.city = 'London') s ORDER BY s.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE d.floor < 4 ORDER BY e.name LIMIT 2;

SELECT * FROM dept d ORDER BY d.dname;

SELECT d.dname, count(*) AS n FROM dept d JOIN emp e ON e.dept = d.did GROUP BY d.dname ORDER BY d.dname;
