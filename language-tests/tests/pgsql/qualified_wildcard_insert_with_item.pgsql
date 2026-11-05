/**
[env]
namespace = true
database = true
imports = ["pgsql/_qualified_wildcard.surql"]

[test]
reason = "The * of a table next to another item is refused in the query of an INSERT: no position says which column the item is written to"

[test.results]
request-error = "PostgreSQL INSERT lowering error: unsupported statement: an INSERT ... SELECT that selects the * of a table together with another item"
*/

INSERT INTO qw_dst (k, name, n) SELECT s.*, 5 FROM qw_src s;
