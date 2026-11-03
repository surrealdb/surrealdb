/**
[env]
namespace = true
database = true
imports = ["pgsql/sql_types/_hosts.surql"]

[test]
reason = "a literal compared with a network column that does not read as a network is refused, as PostgreSQL refuses it as input to inet"

[test.results]
request-error = "PostgreSQL SELECT lowering error: invalid input syntax for type: inet: \"not an address\""
*/

SELECT id FROM host WHERE addr = 'not an address';
