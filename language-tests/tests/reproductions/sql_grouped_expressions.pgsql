/**
[env]
namespace = true
database = true
imports = ["pgsql/_id_key_grouped.surql"]

[test]
reason = "Grouped columns are read from aggregation output within functions and membership, including unprojected group keys."

[[test.results]]
value = "[{ id: 2, c: 1 }, { id: 3, c: 1 }]"

[[test.results]]
value = "[{ id: 2, c: 1 }]"

[[test.results]]
value = "[{ id: 3, c: 1 }, { id: 2, c: 1 }, { id: 1, c: 1 }, { id: NONE, c: 2 }]"

[[test.results]]
value = "[{ c: 1 }, { c: 1 }]"
*/
SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id HAVING abs(e.id) > 1 ORDER BY e.id;
SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id HAVING e.id IN (SELECT id FROM emp WHERE id = 2);
SELECT e.id, count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id ORDER BY abs(e.id) DESC NULLS LAST;
SELECT count(*) AS c FROM dept d LEFT JOIN emp e ON e.dept = d.id GROUP BY e.id HAVING abs(e.id) > 1 ORDER BY e.id;
