/**
[env]
namespace = true
database = true

[test]
reason = "Serial INSERT batches into a SCHEMALESS table with a secondary index complete, and every acknowledged row is readable through the index. A scale-dependent stall is RocksDB-specific; this pins result correctness, not scale."
issue = 7536

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 5000 }]"

[[test.results]]
value = "[{ count: 10 }]"
*/

DEFINE TABLE items SCHEMALESS;
DEFINE INDEX idx_k ON items FIELDS k;
FOR $b IN 0..5 {
	LET $rows = array::map(<array>0..1000, |$i| { k: $b * 1000 + $i, s: 'payload' });
	INSERT INTO items $rows RETURN NONE;
};
SELECT count() FROM items GROUP ALL;
SELECT count() FROM items WHERE k >= 2000 AND k < 2010 GROUP ALL;
