/**
[env]
namespace = true
database = true
imports = ["pgsql/_intersect_except_tables.surql"]

[test]
reason = "a subquery used as a value answers bare values, not rows with columns to compare, so INTERSECT in one is refused rather than answered wrongly"

[test.results]
request-error = "PostgreSQL SELECT lowering error: unsupported expression: INTERSECT is not supported in a subquery that answers a single value"
*/

SELECT w FROM ie_left WHERE v IN (SELECT v FROM ie_left INTERSECT SELECT v FROM ie_right);
