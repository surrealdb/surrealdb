/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and RocksDB does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "a CREATE over a range of ids that meets a record that exists fails with the error of the first such id, and leaves what the serial run leaves, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: c:500, n: 0 }]"

[[test.results]]
value = "[{ id: c:1700, n: 0 }]"

[[test.results]]
error = "Database record `c:500` already exists"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database record `c:500` already exists"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1201 }]"

*/

DEFINE TABLE c;
CREATE c:500 SET n = 0;
CREATE c:1700 SET n = 0;
CREATE |c:1..2000| SET n = 1 RETURN NONE;
SELECT count() FROM c GROUP ALL;
BEGIN;
CREATE |c:1..2000| SET n = 1 RETURN NONE;
COMMIT;
SELECT count() FROM c GROUP ALL;
CREATE |c:501..1700| SET n = 1 RETURN NONE;
SELECT count() FROM c GROUP ALL;
