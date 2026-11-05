/**
[env]
namespace = true
database = true
# The forced configuration cuts these writes into a batch for each record, which RocksDB does not
# finish in the default timeout while the suite runs in parallel.
timeout = 30000

[test]
reason = "a DELETE purges the edges of each record it deletes, and an event of the edge table writes a record the DELETE has still to read: the DELETE judges that record as the serial run leaves it, whatever width its compute half could run at"

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
value = "[t:100, t:700]"

[[test.results]]
value = "[{ count: 898 }]"

[[test.results]]
value = "[]"

*/

DEFINE TABLE t TYPE NORMAL;
DEFINE TABLE knows TYPE RELATION FROM t TO t;
DEFINE EVENT unfriend ON knows WHEN $event = 'DELETE' THEN { UPDATE $before.out SET friends -= 1 };
FOR $i IN 0..900 { CREATE type::record('t', $i) SET friends = 1, gone = ($i = 100) };
RELATE t:100->knows->t:700 RETURN NONE;
DELETE t WHERE gone OR friends = 0 RETURN VALUE $before.id;
SELECT count() FROM t GROUP ALL;
SELECT * FROM knows;
