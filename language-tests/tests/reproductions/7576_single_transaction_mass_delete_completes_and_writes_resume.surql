/**
[env]
namespace = true
database = true

[test]
reason = "A table-wide DELETE of an indexed table in one transaction commits and later writes proceed. A permanent freeze from an oversized memtable is RocksDB-specific; this pins result correctness, not scale."
issue = 7576

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"
*/

DEFINE TABLE big SCHEMALESS;
DEFINE INDEX idx_k ON big FIELDS k;
CREATE |big:3000| SET k = rand::int(0, 1000) RETURN NONE;
DELETE big RETURN NONE;
SELECT count() FROM big GROUP ALL;
CREATE big:after SET k = 1 RETURN NONE;
SELECT count() FROM big WHERE k = 1 GROUP ALL;
