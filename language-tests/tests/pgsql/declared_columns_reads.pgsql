/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_columns.surql"]

[test]
reason = "PostgreSQL reads over tables that declare their columns: a name the enclosing query owns binds there from inside a subquery, an alias or a merged USING column is not a missing column, a table that does not declare its columns reads any name, and * names the declared columns"

[[test.results]]
value = "[{ dept: 1, id: 1, name: 'Ann', note: 'vip' }, { dept: 2, id: 2, name: 'Ben', note: NONE }, { dept: NONE, id: 3, name: 'Cy', note: NONE }, { dept: 9, id: 4, name: 'Di', note: NONE }]"

[[test.results]]
value = "[{ label: 'Eng', name: 'Ann' }, { label: 'Ops', name: 'Ben' }, { label: NONE, name: 'Cy' }, { label: NONE, name: 'Di' }]"

[[test.results]]
value = "[{ name: 'Ann' }, { name: 'Ben' }]"

[[test.results]]
value = "[{ name: 'Ann' }]"

[[test.results]]
value = "[{ x: 4 }, { x: 3 }, { x: 2 }, { x: 1 }]"

[[test.results]]
value = "[{ d: 1, n: 1 }, { d: 2, n: 1 }, { d: 9, n: 1 }, { d: NONE, n: 1 }]"

[[test.results]]
value = "[{ id: 1, label: 'Eng' }, { id: 2, label: 'Ops' }]"

[[test.results]]
value = "[{ label: 'Eng', name: 'Ann' }, { label: 'Ops', name: 'Ben' }]"

[[test.results]]
value = "[{ name: 'Cy' }]"

[[test.results]]
value = "[{ name: 'Ann' }]"

[[test.results]]
value = "[{ extra: 'e' }, { extra: NONE }]"

[[test.results]]
value = "[{ extra: 'e', id: 1, name: 'Ann' }, { id: 2, name: 'Ben' }]"

[[test.results]]
value = "[{ extra: 'e', name: 'Ann' }, { extra: NONE, name: 'Ben' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 4 }]"
*/

SELECT * FROM declcol_emp ORDER BY id;

SELECT name, (SELECT d.label FROM declcol_dept d WHERE d.id = dept) AS label FROM declcol_emp ORDER BY id;

SELECT name FROM declcol_emp WHERE EXISTS (SELECT 1 FROM declcol_dept d WHERE d.id = dept) ORDER BY id;

SELECT name FROM declcol_emp WHERE dept IN (SELECT id FROM declcol_dept WHERE label = 'Eng') ORDER BY id;

SELECT id AS x FROM declcol_emp ORDER BY x DESC;

SELECT dept AS d, count(*) AS n FROM declcol_emp GROUP BY d ORDER BY d;

SELECT id, label FROM declcol_emp JOIN declcol_dept USING (id) ORDER BY id;

SELECT name, label FROM declcol_emp e JOIN declcol_dept d ON e.dept = d.id ORDER BY name;

WITH c AS (SELECT name, dept FROM declcol_emp) SELECT name FROM c WHERE dept IS NULL ORDER BY name;

SELECT s.name FROM (SELECT name, note FROM declcol_emp) s WHERE s.note IS NOT NULL;

SELECT extra FROM declcol_loose ORDER BY id;

SELECT * FROM declcol_loose ORDER BY id;

SELECT e.name, extra FROM declcol_emp e JOIN declcol_loose l ON l.name = e.name;

SELECT * FROM declcol_empty;

SELECT count(*) AS n FROM declcol_emp;
