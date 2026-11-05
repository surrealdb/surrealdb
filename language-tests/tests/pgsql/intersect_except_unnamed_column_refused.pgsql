/**
[env]
namespace = true
database = true
imports = ["pgsql/_intersect_except_tables.surql"]

[test]
reason = "a computed column of a derived table's first query has no name to compare it by, so EXCEPT over it is refused until an alias names it"

[test.results]
request-error = "PostgreSQL SELECT lowering error: unsupported expression: EXCEPT needs a name for every column of its first query; give each computed column an alias with AS"
*/

SELECT count(*) AS n FROM (SELECT v + 1 FROM ie_left EXCEPT SELECT v + 1 FROM ie_right) s;
