/**
[env]
namespace = true
database = true
imports = ["pgsql/_scalar_rows.surql"]

[test]
reason = "A subquery used as a value has one column"

[test.results]
request-error = "PostgreSQL SELECT lowering error: planning error: subquery must return only one column"
*/

SELECT (SELECT k, name FROM sr_a WHERE k = 1) AS x;
