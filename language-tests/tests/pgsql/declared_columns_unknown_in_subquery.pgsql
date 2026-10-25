/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_columns.surql"]

[test]
reason = "A column that neither a subquery's tables nor any enclosing query has is an error, however deep the subquery"

[test.results]
request-error = "PostgreSQL SELECT lowering error: column \"nosuch\" does not exist"
*/

SELECT name FROM declcol_emp WHERE EXISTS (SELECT 1 FROM declcol_dept WHERE label = nosuch);
