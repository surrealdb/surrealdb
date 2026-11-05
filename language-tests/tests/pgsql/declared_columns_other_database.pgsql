/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_columns.surql"]

[test]
reason = "A table that another database qualifies is refused, not read as the table of the session's own database that has its name, when the table declares its columns"

[test.results]
request-error = "PostgreSQL SELECT lowering error: planning error: cross-database queries not supported: otherdb.public.declcol_emp"
*/

SELECT name FROM otherdb.public.declcol_emp;
