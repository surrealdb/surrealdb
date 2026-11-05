/**
[env]
namespace = true
database = true
imports = ["pgsql/_id_key_grouped.surql"]

[test]
reason = "Grouping by the id of the outer-joined table of a PostgreSQL query puts the departments without employees in one group whose id is absent, as NULL is in PostgreSQL. HAVING and an ORDER BY expression over that id read it without raising: the group is found by IS NULL and kept out of an ordering comparison, and ORDER BY puts NULL last."

[[test.results]]
value = "[{ c: 2, id: NONE }]"

[[test.results]]
value = "[{ c: 1, id: 2 }, { c: 1, id: 3 }]"

[[test.results]]
value = "[{ c: 1, id: 1 }, { c: 1, id: 2 }, { c: 1, id: 3 }, { c: 2, id: NONE }]"

[[test.results]]
value = "[{ c: 1, id: 2 }]"

[[test.results]]
value = "[{ c: 1, emp: NONE, id: 3 }, { c: 1, emp: NONE, id: 4 }]"

[[test.results]]
value = "[{ c: 1, id: 1 }, { c: 1, id: 2 }, { c: 1, id: 3 }, { c: 2, id: NONE }]"
*/

SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id HAVING e.id IS NULL;

SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id HAVING e.id IS NOT NULL AND e.id > 1 ORDER BY e.id;

SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id ORDER BY e.id + 0;

SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id HAVING e.id = 2;

SELECT d.id, e.id AS emp, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY d.id, e.id HAVING e.id IS NULL ORDER BY d.id;

SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id ORDER BY e.id;
