/**
[env]
namespace = true
database = true
imports = ["pgsql/_native.surql"]

[test]
reason = "PostgreSQL joins under paging, distinct, aggregation and set operations, with sides that are statements, joins and empty tables"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }]"

[[test.results]]
value = "[{ dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }]"

[[test.results]]
value = "[{ dname: 'Eng' }, { dname: 'Eng' }, { dname: 'Keyless' }, { dname: 'Nowhere' }, { dname: 'Ops' }, { dname: 'Ops' }, { dname: 'Ops2' }, { dname: 'Ops2' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }]"

[[test.results]]
value = "[{ n: 8 }]"

[[test.results]]
value = "[{ n: 10 }]"

[[test.results]]
value = "[{ n: 8 }]"

[[test.results]]
value = "[{ n: 0 }]"

[[test.results]]
value = "[{ pay: 0 }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2, pay: 180 }, { dname: 'Keyless', n: 1, pay: 50 }, { dname: 'Nowhere', n: 1, pay: 60 }, { dname: 'Ops', n: 2, pay: 210.5f }, { dname: 'Ops2', n: 2, pay: 210.5f }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2 }, { dname: 'Keyless', n: 1 }, { dname: 'Nowhere', n: 1 }, { dname: 'Ops', n: 2 }, { dname: 'Ops2', n: 2 }, { dname: NONE, n: 2 }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2 }, { dname: 'Ops', n: 2 }, { dname: 'Ops2', n: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Ops', pay: 210.5f }, { dname: 'Ops2', pay: 210.5f }, { dname: 'Eng', pay: 180 }, { dname: 'Nowhere', pay: 60 }, { dname: 'Keyless', pay: 50 }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2 }, { dname: 'Ops', n: 2 }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }]"

[[test.results]]
value = "[{ n: 'Alice' }, { n: 'Bob' }, { n: 'Cara' }, { n: 'Cara' }, { n: 'Dan' }, { n: 'Dan' }, { n: 'Eng' }, { n: 'Finn' }, { n: 'Gus' }, { n: 'Keyless' }, { n: 'Legal' }, { n: 'Nowhere' }, { n: 'Ops' }, { n: 'Ops2' }]"

[[test.results]]
value = "[{ n: 'Bob' }, { n: 'Cara' }, { n: 'Dan' }, { n: 'Eve' }]"

[[test.results]]
value = "[{ dname: 'Ops', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2 }, { dname: 'Keyless', n: 1 }, { dname: 'Nowhere', n: 1 }, { dname: 'Ops', n: 2 }, { dname: 'Ops2', n: 2 }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Keyless', name: 'Gus' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops2', name: 'Dan' }, { dname: NONE, name: 'Eve' }, { dname: NONE, name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }, { dname: NONE, name: 'Hana' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: NONE, name: 'Alice' }, { n: NONE, name: 'Bob' }, { n: NONE, name: 'Cara' }, { n: NONE, name: 'Dan' }, { n: NONE, name: 'Eve' }, { n: NONE, name: 'Finn' }, { n: NONE, name: 'Gus' }, { n: NONE, name: 'Hana' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Nowhere', name: 'Finn' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: NONE, name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: NONE, name: 'Dan' }, { dname: NONE, name: 'Eve' }, { dname: 'Nowhere', name: 'Finn' }, { dname: NONE, name: 'Gus' }, { dname: NONE, name: 'Hana' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Keyless', name: 'Alice' }, { dname: 'Legal', name: 'Alice' }, { dname: 'Nowhere', name: 'Alice' }, { dname: 'Ops', name: 'Alice' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: NONE, name: 'Alice' }, { dname: NONE, name: 'Bob' }, { dname: NONE, name: 'Cara' }, { dname: NONE, name: 'Dan' }, { dname: NONE, name: 'Eve' }, { dname: NONE, name: 'Finn' }, { dname: NONE, name: 'Gus' }, { dname: NONE, name: 'Hana' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }]"

[[test.results]]
value = "[{ dname: 'Ops', name: 'Cara', net: 110.5f }, { dname: 'Ops2', name: 'Cara', net: 100.5f }, { dname: 'Ops', name: 'Dan', net: 80 }, { dname: 'Eng', name: 'Alice', net: 70 }, { dname: 'Ops2', name: 'Dan', net: 70 }, { dname: 'Nowhere', name: 'Finn', net: 60 }, { dname: 'Eng', name: 'Bob', net: 50 }, { dname: 'Keyless', name: 'Gus', net: -20 }]"

[[test.results]]
value = "[{ dname: 'Eng', level: 'high', name: 'Alice' }, { dname: 'Eng', level: 'high', name: 'Bob' }, { dname: 'Ops', level: 'low', name: 'Cara' }, { dname: 'Ops2', level: 'low', name: 'Cara' }, { dname: 'Ops', level: 'low', name: 'Dan' }, { dname: 'Ops2', level: 'low', name: 'Dan' }, { dname: 'Nowhere', level: 'low', name: 'Finn' }, { dname: 'Keyless', level: 'high', name: 'Gus' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'none', name: 'Eve' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }, { dname: 'none', name: 'Hana' }]"

[[test.results]]
value = "[{ _join0: { did: 1, dname: 'Eng', floor: 3, id: dept:1 }, city: 'London', dept: 1, id: emp:1, name: 'Alice', salary: 100 }, { _join0: { did: 1, dname: 'Eng', floor: 3, id: dept:1 }, city: 'Paris', dept: 1, id: emp:2, mgr: 1, name: 'Bob', salary: 80 }, { _join0: { did: 2, dname: 'Ops', floor: 1, id: dept:2 }, city: 'London', dept: 2, id: emp:3, mgr: 1, name: 'Cara', salary: 120.5f }, { _join0: { did: 2, dname: 'Ops2', floor: 2, id: dept:3 }, city: 'London', dept: 2, id: emp:3, mgr: 1, name: 'Cara', salary: 120.5f }, { _join0: { did: 2, dname: 'Ops', floor: 1, id: dept:2 }, city: 'Berlin', dept: 2, id: emp:4, mgr: 3, name: 'Dan', salary: 90 }, { _join0: { did: 2, dname: 'Ops2', floor: 2, id: dept:3 }, city: 'Berlin', dept: 2, id: emp:4, mgr: 3, name: 'Dan', salary: 90 }, { _join0: { did: NULL, dname: 'Nowhere', floor: 0, id: dept:5 }, city: 'London', dept: NULL, id: emp:6, mgr: 2, name: 'Finn', salary: 60 }, { _join0: { dname: 'Keyless', floor: 7, id: dept:6 }, city: 'Berlin', id: emp:7, mgr: 2, name: 'Gus', salary: 50 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: NONE, name: 'Alice' }, { id: NONE, name: 'Bob' }, { id: NONE, name: 'Cara' }, { id: NONE, name: 'Cara' }, { id: NONE, name: 'Dan' }, { id: NONE, name: 'Dan' }, { id: NONE, name: 'Eve' }, { id: NONE, name: 'Finn' }, { id: NONE, name: 'Gus' }, { id: NONE, name: 'Hana' }]"
*/

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did LIMIT 3;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did LIMIT 3 OFFSET 2;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did LIMIT 0;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did LIMIT 4;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name LIMIT 1;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name LIMIT 0;

SELECT e.name FROM emp e JOIN dept d ON e.dept = d.did WHERE d.floor > 0 LIMIT 2;

SELECT DISTINCT d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY d.dname;

SELECT DISTINCT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name, d.dname;

SELECT DISTINCT e.name FROM emp e JOIN dept d ON e.dept = d.did LIMIT 2;

SELECT count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did;

SELECT count(*) AS n FROM emp e LEFT JOIN dept d ON e.dept = d.did;

SELECT count(d.dname) AS n FROM emp e LEFT JOIN dept d ON e.dept = d.did;

SELECT count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did WHERE d.floor > 100;

SELECT sum(e.salary) AS pay FROM emp e JOIN dept d ON e.dept = d.did WHERE d.floor > 100;

SELECT d.dname, count(*) AS n, sum(e.salary) AS pay FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname ORDER BY d.dname;

SELECT d.dname, count(*) AS n FROM emp e LEFT JOIN dept d ON e.dept = d.did GROUP BY d.dname ORDER BY d.dname;

SELECT d.dname, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname HAVING count(*) > 1 ORDER BY d.dname;

SELECT d.dname, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname HAVING count(*) > 100 ORDER BY d.dname;

SELECT d.dname, sum(e.salary) AS pay FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname ORDER BY sum(e.salary) DESC;

SELECT d.dname, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname ORDER BY n DESC, d.dname LIMIT 2;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did UNION SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY name;

SELECT e.name AS n FROM emp e JOIN dept d ON e.dept = d.did UNION ALL SELECT d.dname AS n FROM dept d ORDER BY n;

SELECT e.name AS n FROM emp e JOIN dept d ON e.dept = d.did WHERE d.floor = 1 UNION SELECT e.name AS n FROM emp e WHERE e.city = 'Paris' ORDER BY n;

SELECT j.name, j.dname FROM (SELECT e.name AS name, d.dname AS dname FROM emp e JOIN dept d ON e.dept = d.did) j WHERE j.dname = 'Ops' ORDER BY j.name;

SELECT j.dname, j.n FROM (SELECT d.dname AS dname, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname) j ORDER BY j.dname;

SELECT e.name, s.dname FROM emp e JOIN (SELECT d.did AS did, d.dname AS dname FROM dept d WHERE d.floor > 1) s ON e.dept = s.did ORDER BY e.name;

SELECT e.name, s.dname FROM emp e LEFT JOIN (SELECT d.did AS did, d.dname AS dname FROM dept d WHERE d.floor > 1) s ON e.dept = s.did ORDER BY e.name;

SELECT e.name, s.n FROM emp e JOIN (SELECT p.emp_id AS emp_id, count(*) AS n FROM proj p GROUP BY p.emp_id) s ON s.emp_id = e.id ORDER BY e.name;

SELECT e.name, s.n FROM emp e LEFT JOIN (SELECT p.emp_id AS emp_id, count(*) AS n FROM proj p GROUP BY p.emp_id) s ON s.emp_id = e.id ORDER BY e.name;

SELECT e.name, d.dname, p.hours, t.label FROM emp e JOIN dept d ON e.dept = d.did JOIN proj p ON p.emp_id = e.id JOIN tag t ON t.k = d.did ORDER BY e.name, p.hours, t.label;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did AND e.city = 'London' ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did AND e.city = 'London' ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did AND e.dept = d.did ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e JOIN dept d ON 1 = 1 ORDER BY e.name, d.dname LIMIT 5;

SELECT e.name, d.dname FROM emp e JOIN dept d ON false ORDER BY e.name;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON false ORDER BY e.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE e.dept IN (1, 2) AND d.floor IN (1, 3) ORDER BY e.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE e.name LIKE 'A%' OR d.dname LIKE 'O%' ORDER BY e.name, d.dname;

SELECT e.name, d.dname, e.salary - d.floor * 10 AS net FROM emp e JOIN dept d ON e.dept = d.did ORDER BY net DESC, e.name;

SELECT e.name, d.dname, CASE WHEN d.floor > 2 THEN 'high' ELSE 'low' END AS level FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name, d.dname;

SELECT e.name, coalesce(d.dname, 'none') AS dname FROM emp e LEFT JOIN dept d ON e.dept = d.did ORDER BY e.name;

SELECT * FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.id, d.id;

SELECT x.id FROM empty x JOIN emp e ON e.dept = x.did JOIN dept d ON d.did = e.dept;

SELECT e.name FROM emp e JOIN dept d ON e.dept = d.did JOIN empty x ON x.did = d.did;

SELECT e.name, x.id FROM emp e LEFT JOIN empty x ON x.did = e.dept LEFT JOIN dept d ON d.did = e.dept ORDER BY e.name;
