/**
[env]
namespace = true
database = true
imports = ["pgsql/_intersect_except_tables.surql"]

[test]
reason = "A column that a row does not have is NULL to INTERSECT and EXCEPT, as a column that an INSERT leaves out is in SQL: a row without it matches a row that holds NULL; the expected rows are those of PostgreSQL 17"

[[test.results]]
value = "[{ a: 1 }]"

[[test.results]]
value = "[{ a: 2 }]"

[[test.results]]
value = "[{ a: 3 }]"
*/

SELECT a FROM (SELECT a, b FROM ie_gap_left INTERSECT SELECT a, b FROM ie_gap_right) s;
SELECT a FROM (SELECT a, b FROM ie_gap_left EXCEPT SELECT a, b FROM ie_gap_right) s;
SELECT a FROM (SELECT a, b FROM ie_gap_right EXCEPT SELECT a, b FROM ie_gap_left) s ORDER BY a;
