/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and SurrealKV does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "an UPDATE and a DELETE over a table whose fields are indexed leave the index entries of the serial run, whatever width their compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 900 }]"

[[test.results]]
value = "[10000, 10001, 10002, 10003, 10004]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 450 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 771 }]"

[[test.results]]
value = "[{ count: 771 }]"

*/

DEFINE TABLE t;
DEFINE INDEX by_n ON t FIELDS n;
DEFINE INDEX by_name ON t FIELDS name;
FOR $i IN 0..900 { CREATE type::record('t', $i) SET n = $i, name = 'n' + <string> $i };
UPDATE t SET n = n + 10000 RETURN NONE;
SELECT count() FROM t WITH INDEX by_n WHERE n >= 10000 GROUP ALL;
SELECT VALUE n FROM t WITH INDEX by_n WHERE n >= 10000 LIMIT 5;
UPDATE t SET name = 'x' + <string> n WHERE n % 2 = 0 RETURN NONE;
SELECT count() FROM t WITH INDEX by_name WHERE name >= 'x' GROUP ALL;
DELETE t WHERE n % 7 = 0 RETURN NONE;
SELECT count() FROM t WITH INDEX by_n WHERE n >= 10000 GROUP ALL;
SELECT count() FROM t GROUP ALL;
