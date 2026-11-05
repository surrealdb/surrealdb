/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "The * of a table the query does not read is an error, as in PostgreSQL"

[test.results]
request-error = "PostgreSQL SELECT lowering error: missing FROM-clause entry for table \"o\""
*/

SELECT o.* FROM users u;
