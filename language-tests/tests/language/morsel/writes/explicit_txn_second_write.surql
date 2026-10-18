/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and RocksDB does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "a write that follows another write of its transaction sees what the first wrote and answers as the serial run does, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:9999, n: 9999 }]"

[[test.results]]
value = "[891, 892, 893, 894, 895, 896, 897, 898, 899, 900, 10000]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 851 }]"

[[test.results]]
value = "[881, 883, 885, 887, 889, 891, 893, 895, 897, 899]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5, 6, 7]"

*/

DEFINE TABLE t;
FOR $i IN 0..900 { CREATE type::record('t', $i) SET n = $i };
BEGIN;
CREATE t:9999 SET n = 9999;
UPDATE t SET n = n + 1 WHERE n >= 890 RETURN VALUE n;
DELETE t WHERE n % 2 = 0 AND n > 800 RETURN NONE;
COMMIT;
SELECT count() FROM t GROUP ALL;
SELECT VALUE n FROM t WHERE n > 880;
BEGIN;
UPDATE t SET n = n + 1 WHERE n < 3 RETURN NONE;
UPDATE t SET n = n + 1 WHERE n < 6 RETURN VALUE n;
CANCEL;
SELECT VALUE n FROM t WHERE n < 8;
