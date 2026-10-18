/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and RocksDB does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "an UPDATE whose compute half fails for two records raises the failure of the first of them and leaves every record as it was, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:2, n: 2, s: 'x' }]"

[[test.results]]
value = "[{ id: t:700, n: 700, s: 'y' }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[{ count: 900 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `uniq` already contains 1, with record `t:49`"

[[test.results]]
value = "[{ count: 0 }]"

*/

DEFINE TABLE t;
FOR $i IN 0..900 { CREATE type::record('t', $i) SET n = $i, s = <string> $i };
UPDATE t:2 SET s = 'x';
UPDATE t:700 SET s = 'y';
UPDATE t SET i = <int> s;
SELECT count() FROM t WHERE i != NONE GROUP ALL;
DELETE t WHERE <int> s > 0;
SELECT count() FROM t GROUP ALL;
DEFINE INDEX uniq ON t FIELDS u UNIQUE;
UPDATE t SET u = 1, i = <int> s WHERE n >= 49;
SELECT count() FROM t WHERE u != NONE GROUP ALL;
