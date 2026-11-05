/**
[env]
namespace = true
database = true
imports = ["pgsql/_union_tables.surql"]

[test]
reason = "The queries of a set operation must have the same number of columns"

[test.results]
request-error = "PostgreSQL SELECT lowering error: planning error: each query of a set operation must have the same number of columns"
*/

SELECT name FROM un_a UNION ALL SELECT label, k2 FROM un_b;
