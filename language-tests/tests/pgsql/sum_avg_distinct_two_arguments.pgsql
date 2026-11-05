/**
[env]
namespace = true
database = true
imports = ["pgsql/_distinct_sums.surql"]

[test]
reason = "A DISTINCT sum of two arguments is refused"

[test.results]
request-error = "PostgreSQL SELECT lowering error: function \"sum\" requires at most 1 arguments, but 2 were provided"
*/

SELECT sum(DISTINCT v, w) FROM ds_t;
