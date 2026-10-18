/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and RocksDB does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "an UPDATE whose value reads the table it updates sees the records as the serial run sees them, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, x: 1 }]"

[[test.results]]
value = "[{ id: t:2, x: 2 }]"

[[test.results]]
value = "[{ id: t:3, x: 3 }]"

[[test.results]]
value = "[6, 11, 20]"

[[test.results]]
value = "[6, 11, 20]"

[[test.results]]
value = "[3, 3, 3]"

*/

DEFINE TABLE t;
CREATE t:1 SET x = 1;
CREATE t:2 SET x = 2;
CREATE t:3 SET x = 3;
UPDATE t SET x = (SELECT VALUE math::sum(x) FROM t GROUP ALL)[0] RETURN VALUE x;
SELECT VALUE x FROM t;
UPDATE t SET x = array::len((SELECT VALUE id FROM t WHERE x > 0)) RETURN VALUE x;
