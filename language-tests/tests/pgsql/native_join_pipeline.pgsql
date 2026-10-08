/**
[env]
namespace = true
database = true
imports = ["pgsql/_native.surql"]

[test]
reason = "PostgreSQL clauses over joins: aggregates, grouping, having, distinct, ordering and paging"

[[test.results]]
value = "[{ n: 8 }]"

[[test.results]]
value = "[{ n: 10 }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2 }, { dname: 'Keyless', n: 1 }, { dname: 'Nowhere', n: 1 }, { dname: 'Ops', n: 2 }, { dname: 'Ops2', n: 2 }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2, total: 180 }, { dname: 'Keyless', n: 1, total: 50 }, { dname: 'Nowhere', n: 1, total: 60 }, { dname: 'Ops', n: 2, total: 210.5f }, { dname: 'Ops2', n: 2, total: 210.5f }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2 }, { dname: 'Ops', n: 2 }, { dname: 'Ops2', n: 2 }]"

[[test.results]]
value = "[{ dname: 'Ops', total: 210.5f }, { dname: 'Ops2', total: 210.5f }, { dname: 'Eng', total: 180 }]"

[[test.results]]
value = "[{ dname: 'Eng', high: 100, low: 80, mean: 90f }, { dname: 'Keyless', high: 50, low: 50, mean: 50f }, { dname: 'Nowhere', high: 60, low: 60, mean: 60f }, { dname: 'Ops', high: 120.5f, low: 90, mean: 105.25f }, { dname: 'Ops2', high: 120.5f, low: 90, mean: 105.25f }]"

[[test.results]]
value = "[{ city: 'Berlin', dname: 'Keyless', n: 1 }, { city: 'Berlin', dname: 'Ops', n: 1 }, { city: 'Berlin', dname: 'Ops2', n: 1 }, { city: 'London', dname: 'Eng', n: 1 }, { city: 'London', dname: 'Nowhere', n: 1 }, { city: 'London', dname: 'Ops', n: 1 }, { city: 'London', dname: 'Ops2', n: 1 }, { city: 'Paris', dname: 'Eng', n: 1 }]"

[[test.results]]
value = "[{ name: 'Alice', projects: 0 }, { name: 'Bob', projects: 0 }, { name: 'Cara', projects: 0 }, { name: 'Dan', projects: 0 }, { name: 'Eve', projects: 0 }, { name: 'Finn', projects: 0 }, { name: 'Gus', projects: 0 }, { name: 'Hana', projects: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Eng', name: 'Alice' }]"

[[test.results]]
value = "[{ dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }]"

[[test.results]]
value = "[{ dname: 'Ops2', name: 'Cara' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }, { dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }]"

[[test.results]]
value = "[{ dname: NONE, name: 'Eve' }, { dname: NONE, name: 'Hana' }, { dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Keyless', name: 'Gus' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops2', name: 'Dan' }]"

[[test.results]]
value = "[{ dname: 'Eng' }, { dname: 'Eng' }, { dname: 'Keyless' }, { dname: 'Nowhere' }, { dname: 'Ops' }, { dname: 'Ops' }, { dname: 'Ops2' }, { dname: 'Ops2' }]"

[[test.results]]
value = "[{ city: 'Berlin' }, { city: 'Berlin' }, { city: 'Berlin' }, { city: 'London' }, { city: 'London' }, { city: 'London' }, { city: 'London' }, { city: 'Paris' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }]"

[[test.results]]
value = "[{ dname: 'Eng', doubled: 200, name: 'Alice' }, { dname: 'Ops', doubled: 241f, name: 'Cara' }, { dname: 'Ops2', doubled: 241f, name: 'Cara' }]"

[[test.results]]
value = "[{ cities: 3 }]"

[[test.results]]
value = "[{ city: 'London', n: 4 }, { city: 'Berlin', n: 3 }]"

[[test.results]]
value = "[{ name: 'Finn' }, { name: 'Cara' }, { name: 'Dan' }, { name: 'Cara' }, { name: 'Dan' }, { name: 'Alice' }, { name: 'Bob' }, { name: 'Gus' }]"

[[test.results]]
value = "[{ name: 'Alice', shout: 'ENG' }, { name: 'Bob', shout: 'ENG' }, { name: 'Cara', shout: 'OPS' }, { name: 'Cara', shout: 'OPS2' }, { name: 'Dan', shout: 'OPS' }, { name: 'Dan', shout: 'OPS2' }, { name: 'Finn', shout: 'NOWHERE' }, { name: 'Gus', shout: 'KEYLESS' }]"

[[test.results]]
value = "[{ dname: 'Eng', n: 2 }, { dname: 'Keyless', n: 1 }, { dname: 'Nowhere', n: 1 }, { dname: 'Ops', n: 2 }, { dname: 'Ops2', n: 2 }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }]"

[[test.results]]
value = "[]"
*/

SELECT count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did;

SELECT count(*) AS n FROM emp e LEFT JOIN dept d ON e.dept = d.did;

SELECT d.dname, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname ORDER BY d.dname;

SELECT d.dname, count(*) AS n, sum(e.salary) AS total FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname ORDER BY d.dname;

SELECT d.dname, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname HAVING count(*) > 1 ORDER BY d.dname;

SELECT d.dname, sum(e.salary) AS total FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname HAVING sum(e.salary) > 150 ORDER BY total DESC;

SELECT d.dname, avg(e.salary) AS mean, min(e.salary) AS low, max(e.salary) AS high FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname ORDER BY d.dname;

SELECT e.city, d.dname, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY e.city, d.dname ORDER BY e.city, d.dname;

SELECT e.name, count(p.pid) AS projects FROM emp e LEFT JOIN proj p ON p.emp_id = e.id GROUP BY e.name ORDER BY e.name;

SELECT e.name, sum(p.hours) AS hours FROM emp e JOIN proj p ON p.emp_id = e.id GROUP BY e.name HAVING sum(p.hours) >= 20 ORDER BY e.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.salary DESC LIMIT 3;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name LIMIT 2 OFFSET 2;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name LIMIT 0;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name OFFSET 100;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did LIMIT 2;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY d.dname DESC, e.name ASC;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did ORDER BY d.dname ASC NULLS FIRST, e.name;

SELECT DISTINCT d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY d.dname;

SELECT DISTINCT e.city FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.city;

SELECT DISTINCT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name, d.dname LIMIT 3;

SELECT e.name, d.dname, e.salary * 2 AS doubled FROM emp e JOIN dept d ON e.dept = d.did WHERE e.salary * 2 > 180 ORDER BY doubled;

SELECT count(DISTINCT e.city) AS cities FROM emp e JOIN dept d ON e.dept = d.did;

SELECT e.city, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY e.city HAVING count(*) >= 2 ORDER BY n DESC, e.city;

SELECT e.name FROM emp e JOIN dept d ON e.dept = d.did ORDER BY d.floor, e.name;

SELECT e.name, upper(d.dname) AS shout FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name;

SELECT s.dname, s.n FROM (SELECT d.dname AS dname, count(*) AS n FROM emp e JOIN dept d ON e.dept = d.did GROUP BY d.dname) s ORDER BY s.dname;

SELECT s.name, d.dname FROM (SELECT e.name AS name, e.dept AS dept FROM emp e WHERE e.salary > 75) s JOIN dept d ON s.dept = d.did ORDER BY s.name, d.dname;

SELECT e.name, s.total FROM emp e JOIN (SELECT p.emp_id AS emp_id, sum(p.hours) AS total FROM proj p GROUP BY p.emp_id) s ON s.emp_id = e.id ORDER BY e.name;
