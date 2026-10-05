/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL aggregate COUNT and SUM queries"

[[test.results]]
value = "[{ cnt: 3, total_sum: 450 }]"
*/

SELECT count(*) AS cnt, sum(o.total) AS total_sum FROM orders o;
