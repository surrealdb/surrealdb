/**
[env]
namespace = true
database = true
imports = ["pgsql/_qualified_wildcard.surql"]

[test]
reason = "The * of a table the query of an INSERT does not read is an error, as in PostgreSQL"

[test.results]
request-error = "PostgreSQL INSERT lowering error: missing FROM-clause entry for table \"x\""
*/

INSERT INTO qw_dst SELECT x.* FROM qw_src s;
