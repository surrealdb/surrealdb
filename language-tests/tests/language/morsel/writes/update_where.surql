/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and SurrealKV does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "an UPDATE with a condition changes the records the condition admits and leaves the others, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 300 }]"

[[test.results]]
value = "[{ total: 704550 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ total: 704700 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "{ id: t:1, k: 7, n: 1 }"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

*/

DEFINE TABLE t;
FOR $i IN 0..900 { CREATE type::record('t', $i) SET n = $i, k = $i % 3 };
UPDATE t SET n = n + 1000 WHERE k = 0 RETURN NONE;
SELECT count() FROM t WHERE n >= 1000 GROUP ALL;
SELECT math::sum(n) AS total FROM t GROUP ALL;
UPDATE t SET n = n + 1 WHERE n % 2 = 0 AND k = 1 RETURN NONE;
SELECT math::sum(n) AS total FROM t GROUP ALL;
UPDATE t SET k = 9 WHERE n > 100000 RETURN NONE;
SELECT count() FROM t WHERE k = 9 GROUP ALL;
UPDATE ONLY t SET k = 7 WHERE n = 1;
UPDATE ONLY t SET k = 8 WHERE n < 10;
