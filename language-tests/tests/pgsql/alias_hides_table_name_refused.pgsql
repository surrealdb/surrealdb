/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "A table that is given an alias is reachable only by that alias, as in PostgreSQL"

[test.results]
request-error = "PostgreSQL SELECT lowering error: missing FROM-clause entry for table \"users\""
*/

SELECT users.name FROM users u;
