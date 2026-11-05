/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "A WHERE clause cannot name an aliased table by its own name, as in PostgreSQL"

[test.results]
request-error = "PostgreSQL SELECT lowering error: missing FROM-clause entry for table \"users\""
*/

SELECT u.name FROM users u WHERE users.age > 20;
