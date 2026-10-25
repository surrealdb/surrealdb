/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_columns.surql"]

[test]
reason = "A column a table that declares all its columns does not have is an error that names only the column the query used"

[test.results]
request-error = "PostgreSQL SELECT lowering error: column \"nosuch\" does not exist"
*/

SELECT nosuch FROM declcol_emp;
