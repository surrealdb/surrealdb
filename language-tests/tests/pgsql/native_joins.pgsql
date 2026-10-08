/**
[env]
namespace = true
database = true
imports = ["pgsql/_native.surql"]

[test]
reason = "PostgreSQL joins: inner and left, key shapes, residual conditions, null and mixed keys, empty sides, chains and self joins"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: NONE, name: 'Eve' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }, { dname: NONE, name: 'Hana' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Keyless', name: 'Gus' }, { dname: 'Legal', name: NONE }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops2', name: 'Dan' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Keyless', name: 'Gus' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops2', name: 'Dan' }, { dname: NONE, name: 'Eve' }, { dname: NONE, name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }, { dname: NONE, name: 'Hana' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Nowhere', name: 'Alice' }, { dname: 'Ops', name: 'Alice' }, { dname: 'Ops2', name: 'Alice' }, { dname: 'Nowhere', name: 'Bob' }, { dname: 'Ops', name: 'Bob' }, { dname: 'Ops2', name: 'Bob' }, { dname: 'Eng', name: 'Cara' }, { dname: 'Nowhere', name: 'Cara' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Nowhere', name: 'Dan' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Nowhere', name: 'Eve' }, { dname: 'Ops', name: 'Eve' }, { dname: 'Ops2', name: 'Eve' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Ops', name: 'Finn' }, { dname: 'Nowhere', name: 'Gus' }, { dname: 'Ops', name: 'Gus' }, { dname: 'Eng', name: 'Hana' }, { dname: 'Nowhere', name: 'Hana' }, { dname: 'Ops', name: 'Hana' }, { dname: 'Ops2', name: 'Hana' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Nowhere', name: 'Alice' }, { dname: 'Ops', name: 'Alice' }, { dname: 'Ops2', name: 'Alice' }, { dname: 'Nowhere', name: 'Bob' }, { dname: 'Ops', name: 'Bob' }, { dname: 'Ops2', name: 'Bob' }, { dname: 'Eng', name: 'Cara' }, { dname: 'Nowhere', name: 'Cara' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Nowhere', name: 'Dan' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Nowhere', name: 'Eve' }, { dname: 'Ops', name: 'Eve' }, { dname: 'Ops2', name: 'Eve' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Ops', name: 'Finn' }, { dname: 'Nowhere', name: 'Gus' }, { dname: 'Ops', name: 'Gus' }, { dname: 'Eng', name: 'Hana' }, { dname: 'Nowhere', name: 'Hana' }, { dname: 'Ops', name: 'Hana' }, { dname: 'Ops2', name: 'Hana' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Keyless', name: 'Alice' }, { dname: 'Legal', name: 'Alice' }, { dname: 'Nowhere', name: 'Alice' }, { dname: 'Ops', name: 'Alice' }, { dname: 'Ops2', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Keyless', name: 'Bob' }, { dname: 'Legal', name: 'Bob' }, { dname: 'Nowhere', name: 'Bob' }, { dname: 'Ops', name: 'Bob' }, { dname: 'Ops2', name: 'Bob' }, { dname: 'Eng', name: 'Cara' }, { dname: 'Keyless', name: 'Cara' }, { dname: 'Legal', name: 'Cara' }, { dname: 'Nowhere', name: 'Cara' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Eng', name: 'Dan' }, { dname: 'Keyless', name: 'Dan' }, { dname: 'Legal', name: 'Dan' }, { dname: 'Nowhere', name: 'Dan' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Eng', name: 'Eve' }, { dname: 'Keyless', name: 'Eve' }, { dname: 'Legal', name: 'Eve' }, { dname: 'Nowhere', name: 'Eve' }, { dname: 'Ops', name: 'Eve' }, { dname: 'Ops2', name: 'Eve' }, { dname: 'Eng', name: 'Finn' }, { dname: 'Keyless', name: 'Finn' }, { dname: 'Legal', name: 'Finn' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Ops', name: 'Finn' }, { dname: 'Ops2', name: 'Finn' }, { dname: 'Eng', name: 'Gus' }, { dname: 'Keyless', name: 'Gus' }, { dname: 'Legal', name: 'Gus' }, { dname: 'Nowhere', name: 'Gus' }, { dname: 'Ops', name: 'Gus' }, { dname: 'Ops2', name: 'Gus' }, { dname: 'Eng', name: 'Hana' }, { dname: 'Keyless', name: 'Hana' }, { dname: 'Legal', name: 'Hana' }, { dname: 'Nowhere', name: 'Hana' }, { dname: 'Ops', name: 'Hana' }, { dname: 'Ops2', name: 'Hana' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ hours: NONE, name: 'Alice' }, { hours: NONE, name: 'Bob' }, { hours: NONE, name: 'Cara' }, { hours: NONE, name: 'Dan' }, { hours: NONE, name: 'Eve' }, { hours: NONE, name: 'Finn' }, { hours: NONE, name: 'Gus' }, { hours: NONE, name: 'Hana' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ boss: NONE, name: 'Alice' }, { boss: NONE, name: 'Bob' }, { boss: NONE, name: 'Cara' }, { boss: NONE, name: 'Dan' }, { boss: NONE, name: 'Eve' }, { boss: NONE, name: 'Finn' }, { boss: NONE, name: 'Gus' }, { boss: NONE, name: 'Hana' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Eng', hours: NONE, name: 'Alice' }, { dname: 'Eng', hours: NONE, name: 'Bob' }, { dname: 'Ops', hours: NONE, name: 'Cara' }, { dname: 'Ops2', hours: NONE, name: 'Cara' }, { dname: 'Ops', hours: NONE, name: 'Dan' }, { dname: 'Ops2', hours: NONE, name: 'Dan' }, { dname: NONE, hours: NONE, name: 'Eve' }, { dname: 'Nowhere', hours: NONE, name: 'Finn' }, { dname: 'Keyless', hours: NONE, name: 'Gus' }, { dname: NONE, hours: NONE, name: 'Hana' }]"

[[test.results]]
value = "[{ dname: 'Eng', hours: NONE, name: 'Alice' }, { dname: 'Eng', hours: NONE, name: 'Bob' }, { dname: 'Ops', hours: NONE, name: 'Cara' }, { dname: 'Ops2', hours: NONE, name: 'Cara' }, { dname: 'Ops', hours: NONE, name: 'Dan' }, { dname: 'Ops2', hours: NONE, name: 'Dan' }, { dname: 'Nowhere', hours: NONE, name: 'Finn' }, { dname: 'Keyless', hours: NONE, name: 'Gus' }]"

[[test.results]]
value = "[{ label: 'dec one', other: 'dec one' }, { label: 'dec one', other: 'float one' }, { label: 'dec one', other: 'int one' }, { label: 'float one', other: 'dec one' }, { label: 'float one', other: 'float one' }, { label: 'float one', other: 'int one' }, { label: 'int one', other: 'dec one' }, { label: 'int one', other: 'float one' }, { label: 'int one', other: 'int one' }, { label: 'none', other: 'none' }, { label: 'null', other: 'null' }, { label: 'string one', other: 'string one' }, { label: 'two', other: 'two' }]"

[[test.results]]
value = "[{ label: 'dec one', other: 'dec one' }, { label: 'dec one', other: 'float one' }, { label: 'dec one', other: 'int one' }, { label: 'float one', other: 'dec one' }, { label: 'float one', other: 'float one' }, { label: 'float one', other: 'int one' }, { label: 'int one', other: 'dec one' }, { label: 'int one', other: 'float one' }, { label: 'int one', other: 'int one' }, { label: 'none', other: 'none' }, { label: 'null', other: 'null' }, { label: 'string one', other: 'string one' }, { label: 'two', other: 'two' }]"

[[test.results]]
value = "[{ label: 'dec one', name: 'Alice' }, { label: 'dec one', name: 'Bob' }, { label: 'float one', name: 'Alice' }, { label: 'float one', name: 'Bob' }, { label: 'int one', name: 'Alice' }, { label: 'int one', name: 'Bob' }, { label: 'none', name: 'Gus' }, { label: 'null', name: 'Finn' }, { label: 'two', name: 'Cara' }, { label: 'two', name: 'Dan' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }, { dname: 'Keyless', name: 'Finn' }, { dname: 'Nowhere', name: 'Finn' }, { dname: 'Keyless', name: 'Gus' }, { dname: 'Nowhere', name: 'Gus' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: NONE, name: 'Alice' }, { id: NONE, name: 'Bob' }, { id: NONE, name: 'Cara' }, { id: NONE, name: 'Dan' }, { id: NONE, name: 'Eve' }, { id: NONE, name: 'Finn' }, { id: NONE, name: 'Gus' }, { id: NONE, name: 'Hana' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Nowhere', name: 'Finn' }]"

[[test.results]]
value = "[{ dname: 'Ops', name: 'Cara' }]"

[[test.results]]
value = "[{ dname: NONE, name: 'Eve' }, { dname: NONE, name: 'Hana' }]"
*/

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e INNER JOIN dept d ON d.did = e.dept ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did ORDER BY e.name, d.dname;

SELECT d.dname, e.name FROM dept d LEFT JOIN emp e ON e.dept = d.did ORDER BY d.dname, e.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did AND d.floor > 1 ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did AND d.floor > 1 ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.salary > d.floor * 30 ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.salary > d.floor * 30 ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e CROSS JOIN dept d ORDER BY e.name, d.dname;

SELECT e.name, p.hours FROM emp e JOIN proj p ON p.emp_id = e.id ORDER BY e.name, p.hours;

SELECT e.name, p.hours FROM proj p JOIN emp e ON p.emp_id = e.id ORDER BY e.name, p.hours;

SELECT e.name, p.hours FROM emp e LEFT JOIN proj p ON p.pid = e.id ORDER BY e.name, p.hours;

SELECT e.name, m.name AS boss FROM emp e JOIN emp m ON e.mgr = m.id ORDER BY e.name;

SELECT e.name, m.name AS boss FROM emp e LEFT JOIN emp m ON e.mgr = m.id ORDER BY e.name;

SELECT e.name, d.dname, p.hours FROM emp e JOIN dept d ON e.dept = d.did JOIN proj p ON p.emp_id = e.id ORDER BY e.name, p.hours;

SELECT e.name, d.dname, p.hours FROM emp e LEFT JOIN dept d ON e.dept = d.did LEFT JOIN proj p ON p.emp_id = e.id ORDER BY e.name, d.dname, p.hours;

SELECT e.name, d.dname, p.hours FROM emp e JOIN dept d ON e.dept = d.did LEFT JOIN proj p ON p.emp_id = e.id ORDER BY e.name, d.dname, p.hours;

SELECT t.label, u.label AS other FROM tag t JOIN tag u ON t.k = u.k ORDER BY t.label, u.label;

SELECT t.label, u.label AS other FROM tag t LEFT JOIN tag u ON t.k = u.k ORDER BY t.label, u.label;

SELECT t.label, e.name FROM tag t JOIN emp e ON t.k = e.dept ORDER BY t.label, e.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept + 1 = d.did + 1 ORDER BY e.name, d.dname;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.city = d.dname ORDER BY e.name;

SELECT e.name, x.id FROM emp e JOIN empty x ON e.dept = x.did ORDER BY e.name;

SELECT e.name, x.id FROM emp e LEFT JOIN empty x ON e.dept = x.did ORDER BY e.name;

SELECT x.id, e.name FROM empty x JOIN emp e ON e.dept = x.did;

SELECT x.id, e.name FROM empty x LEFT JOIN emp e ON e.dept = x.did;

SELECT e.name FROM emp e JOIN dept d ON e.dept = d.did WHERE d.floor = 99;

SELECT * FROM proj p JOIN emp e ON p.emp_id = e.id ORDER BY p.pid;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE e.city = 'London' ORDER BY e.name;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE d.dname = 'Ops' AND e.salary > 100;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did WHERE d.dname IS NULL ORDER BY e.name;
