/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_columns.surql"]

[test]
reason = "A column a qualifier names on a table that declares all its columns, and the table has not, is an error"

[test.results]
request-error = "PostgreSQL SELECT lowering error: column \"nosuch\" does not exist"
*/

SELECT e.name FROM declcol_emp e WHERE e.nosuch = 1;
