/**
[env]
namespace = true
database = true
imports = ["pgsql/_intersect_except_tables.surql"]

[test]
reason = "the columns a * stands for are not known when the query is planned, so INTERSECT over one is refused rather than comparing the wrong columns"

[test.results]
request-error = "PostgreSQL SELECT lowering error: unsupported expression: INTERSECT is not supported with * in either query; list the columns"
*/

SELECT * FROM ie_left INTERSECT SELECT * FROM ie_right;
