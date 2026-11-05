/**
[env]
namespace = true
database = true
imports = ["pgsql/_native.surql"]

[test]
reason = "A WHERE predicate that reads only the left row of a join is judged beneath the join, on its left input, where reading the right input raises nothing; each answers the rows the predicate judged over every pairing answers, in its order, for inner and LEFT joins, a predicate no row passes, NULL comparisons and a LIMIT above"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops', name: 'Dan' }, { dname: 'Ops2', name: 'Dan' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: NONE, name: 'Finn' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Bob' }, { dname: 'Ops', name: 'Cara' }, { dname: 'Ops2', name: 'Cara' }, { dname: NONE, name: 'Hana' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Ops', name: 'Cara' }]"

[[test.results]]
value = "[{ dname: 'Eng', name: 'Alice' }, { dname: 'Eng', name: 'Bob' }, { dname: 'Ops2', name: 'Cara' }, { dname: 'Ops2', name: 'Dan' }]"
*/

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE e.salary > 85;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did WHERE e.city = 'London';

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did WHERE e.salary < 0;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE e.dept IS NULL;

SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept = d.did WHERE e.mgr = 1;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE e.salary > 85 LIMIT 2;

SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.did WHERE d.floor > 1;
