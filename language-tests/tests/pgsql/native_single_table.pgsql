/**
[env]
namespace = true
database = true
imports = ["pgsql/_native.surql"]

[test]
reason = "PostgreSQL single-table reads: filters over indexes, aggregates, having, distinct, ordering and paging"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }]"

[[test.results]]
value = "[{ name: 'Cara' }, { name: 'Dan' }, { name: 'Eve' }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }, { name: 'Eve' }]"

[[test.results]]
value = "[{ name: 'Finn' }, { name: 'Gus' }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }, { name: 'Cara' }, { name: 'Dan' }, { name: 'Eve' }, { name: 'Hana' }]"

[[test.results]]
value = "[{ name: 'Alice', salary: 100 }, { name: 'Dan', salary: 90 }, { name: 'Bob', salary: 80 }, { name: 'Eve', salary: 70 }]"

[[test.results]]
value = "[{ name: 'Cara' }, { name: 'Hana' }, { name: 'Alice' }]"

[[test.results]]
value = "[{ name: 'Cara' }, { name: 'Dan' }, { name: 'Eve' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ who: 'Hana' }, { who: 'Gus' }, { who: 'Finn' }, { who: 'Eve' }, { who: 'Dan' }, { who: 'Cara' }, { who: 'Bob' }, { who: 'Alice' }]"

[[test.results]]
value = "[{ name: 'Cara', twice: 241f }, { name: 'Hana', twice: 220 }]"

[[test.results]]
value = "[{ city: 'Berlin' }, { city: 'London' }, { city: 'Paris' }, { city: 'Rome' }]"

[[test.results]]
value = "[{ dept: 1 }, { dept: 2 }, { dept: 3 }, { dept: 9 }, { dept: NONE }, { dept: NULL }]"

[[test.results]]
value = "[{ city: 'London', dept: 1 }, { city: 'Paris', dept: 1 }, { city: 'Berlin', dept: 2 }, { city: 'London', dept: 2 }, { city: 'Paris', dept: 3 }, { city: 'Rome', dept: 9 }, { city: 'Berlin', dept: NONE }, { city: 'London', dept: NULL }]"

[[test.results]]
value = "[{ city: 'Berlin' }, { city: 'London' }]"

[[test.results]]
value = "[{ city: 'London' }, { city: 'Paris' }, { city: 'Rome' }]"

[[test.results]]
value = "[{ city: 'London' }, { city: 'Paris' }, { city: 'Berlin' }, { city: 'Rome' }]"

[[test.results]]
value = "[{ n: 8 }]"

[[test.results]]
value = "[{ n: 2 }]"

[[test.results]]
value = "[{ n: 0 }]"

[[test.results]]
value = "[{ total: NULL }]"

[[test.results]]
value = "[{ d: 4, hi: 120.5f, lo: 50, n: 6, total: 680.5f }]"

[[test.results]]
value = "[{ dept: 1, n: 2 }, { dept: 2, n: 2 }, { dept: 3, n: 1 }, { dept: 9, n: 1 }, { dept: NONE, n: 1 }, { dept: NULL, n: 1 }]"

[[test.results]]
value = "[{ city: 'Berlin', n: 2, total: 140 }, { city: 'London', n: 3, total: 280.5f }, { city: 'Paris', n: 2, total: 150 }, { city: 'Rome', n: 1, total: 110 }]"

[[test.results]]
value = "[{ city: 'Berlin', n: 2 }, { city: 'London', n: 3 }, { city: 'Paris', n: 2 }]"

[[test.results]]
value = "[{ city: 'London', total: 280.5f }]"

[[test.results]]
value = "[{ city: 'London', n: 3 }]"

[[test.results]]
value = "[{ city: 'Berlin' }, { city: 'London' }, { city: 'Paris' }]"

[[test.results]]
value = "[{ city: 'London', n: 3 }, { city: 'Berlin', n: 2 }, { city: 'Paris', n: 2 }, { city: 'Rome', n: 1 }]"

[[test.results]]
value = "[{ city: 'London', n: 3 }, { city: 'Berlin', n: 2 }]"

[[test.results]]
value = "[{ city: 'London', dept: 1, n: 1 }, { city: 'Paris', dept: 1, n: 1 }, { city: 'Berlin', dept: 2, n: 1 }, { city: 'London', dept: 2, n: 1 }, { city: 'Paris', dept: 3, n: 1 }, { city: 'Rome', dept: 9, n: 1 }, { city: 'Berlin', dept: NONE, n: 1 }, { city: 'London', dept: NULL, n: 1 }]"

[[test.results]]
value = "[{ n: 8 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ one: 1 }]"

[[test.results]]
value = "[{ s: 'x', three: 3 }]"

[[test.results]]
value = "[{ did: 1, dname: 'Eng', floor: 3, id: 1 }, { dname: 'Keyless', floor: 7, id: 6 }, { did: 4, dname: 'Legal', floor: 5, id: 4 }, { did: NULL, dname: 'Nowhere', floor: 0, id: 5 }, { did: 2, dname: 'Ops', floor: 1, id: 2 }, { did: 2, dname: 'Ops2', floor: 2, id: 3 }]"

[[test.results]]
value = "[{ dname: 'Ops' }, { dname: 'Ops2' }]"

[[test.results]]
value = "[{ dname: 'Ops' }, { dname: 'Ops2' }]"

[[test.results]]
value = "[{ label: 'dec one' }, { label: 'float one' }, { label: 'int one' }]"

[[test.results]]
value = "[{ label: 'none' }, { label: 'null' }]"
*/

SELECT e.name FROM emp e WHERE e.dept = 1 ORDER BY e.name;

SELECT e.name FROM emp e WHERE e.dept = 2 OR e.dept = 3 ORDER BY e.name;

SELECT e.name FROM emp e WHERE e.dept IN (1, 3) ORDER BY e.name;

SELECT e.name FROM emp e WHERE e.dept IS NULL ORDER BY e.name;

SELECT e.name FROM emp e WHERE e.dept IS NOT NULL ORDER BY e.name;

SELECT e.name, e.salary FROM emp e WHERE e.salary BETWEEN 70 AND 100 ORDER BY e.salary DESC, e.name;

SELECT e.name FROM emp e ORDER BY e.salary DESC LIMIT 3;

SELECT e.name FROM emp e ORDER BY e.name LIMIT 3 OFFSET 2;

SELECT e.name FROM emp e ORDER BY e.name LIMIT 0;

SELECT e.name AS who FROM emp e ORDER BY who DESC;

SELECT e.name, e.salary * 2 AS twice FROM emp e ORDER BY twice DESC LIMIT 2;

SELECT DISTINCT e.city FROM emp e ORDER BY e.city;

SELECT DISTINCT e.dept FROM emp e ORDER BY e.dept;

SELECT DISTINCT e.dept, e.city FROM emp e ORDER BY e.dept, e.city;

SELECT DISTINCT e.city FROM emp e ORDER BY e.city LIMIT 2;

SELECT DISTINCT e.city FROM emp e ORDER BY e.city OFFSET 1;

SELECT DISTINCT e.city FROM emp e;

SELECT count(*) AS n FROM emp e;

SELECT count(*) AS n FROM emp e WHERE e.dept = 1;

SELECT count(*) AS n FROM empty x;

SELECT sum(x.n) AS total FROM empty x;

SELECT count(e.dept) AS n, count(DISTINCT e.dept) AS d, min(e.salary) AS lo, max(e.salary) AS hi, sum(e.salary) AS total FROM emp e;

SELECT e.dept, count(*) AS n FROM emp e GROUP BY e.dept ORDER BY e.dept;

SELECT e.city, count(*) AS n, sum(e.salary) AS total FROM emp e GROUP BY e.city ORDER BY e.city;

SELECT e.city, count(*) AS n FROM emp e GROUP BY e.city HAVING count(*) > 1 ORDER BY e.city;

SELECT e.city, sum(e.salary) AS total FROM emp e GROUP BY e.city HAVING sum(e.salary) > 150 ORDER BY total;

SELECT e.city, count(*) AS n FROM emp e GROUP BY e.city HAVING sum(e.salary) > 150 ORDER BY e.city;

SELECT e.city FROM emp e GROUP BY e.city HAVING count(*) > 1 ORDER BY e.city;

SELECT e.city, count(*) AS n FROM emp e GROUP BY e.city ORDER BY count(*) DESC, e.city;

SELECT e.city, count(*) AS n FROM emp e GROUP BY e.city ORDER BY n DESC, e.city LIMIT 2;

SELECT e.dept, e.city, count(*) AS n FROM emp e GROUP BY e.dept, e.city ORDER BY e.dept, e.city;

SELECT count(*) AS n FROM emp e HAVING count(*) > 3;

SELECT count(*) AS n FROM emp e HAVING count(*) > 300;

SELECT 1 AS one;

SELECT 1 + 2 AS three, 'x' AS s;

SELECT * FROM dept d ORDER BY d.dname;

SELECT d.dname FROM dept d WHERE d.did = 2 ORDER BY d.dname;

SELECT d.dname FROM dept d WHERE d.did > 1 AND d.floor < 5 ORDER BY d.dname;

SELECT t.label FROM tag t WHERE t.k = 1 ORDER BY t.label;

SELECT t.label FROM tag t WHERE t.k IS NULL ORDER BY t.label;
