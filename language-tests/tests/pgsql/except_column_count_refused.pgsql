/**
[env]
namespace = true
database = true
imports = ["pgsql/_intersect_except_tables.surql"]

[test]
reason = "the two queries of an EXCEPT must have the same number of columns, as in every set operation, so a difference is refused rather than compared on the columns they share"

[test.results]
request-error = "PostgreSQL SELECT lowering error: planning error: each query of a set operation must have the same number of columns"
*/

SELECT v FROM ie_left EXCEPT SELECT v, w FROM ie_right;
