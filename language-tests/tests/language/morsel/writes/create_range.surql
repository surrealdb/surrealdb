/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and RocksDB does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "a CREATE over a range of ids creates the records, indexes them and returns the rows in the order of the ids, as the serial run does, whatever width its compute half runs at"

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
value = "[{ count: 1499 }]"

[[test.results]]
value = "[c:1, c:2, c:3]"

[[test.results]]
value = "[{ n: 2, s: '1' }, { n: 1400, s: '700' }, { n: 2998, s: '1499' }]"

[[test.results]]
value = "[2990, 2992, 2994, 2996, 2998]"

[[test.results]]
value = "[{ id: c:2000 }, { id: c:2001 }, { id: c:2002 }, { id: c:2003 }]"

[[test.results]]
value = "[{ id: c:3000, n: 6000, s: '3000' }, { id: c:3001, n: 6002, s: '3001' }, { id: c:3002, n: 6004, s: '3002' }]"

[[test.results]]
value = "[8000, 8002, 8004]"

[[test.results]]
value = "{ id: c:5000, n: 10000, s: '5000' }"

*/

DEFINE TABLE c;
DEFINE FIELD n ON c VALUE record::id(id) * 2;
DEFINE FIELD s ON c VALUE <string> record::id(id);
DEFINE INDEX by_n ON c FIELDS n;
CREATE |c:1..1500| RETURN NONE;
SELECT count() FROM c GROUP ALL;
SELECT VALUE id FROM c ORDER BY id LIMIT 3;
SELECT n, s FROM c WHERE id IN [c:1, c:700, c:1499] ORDER BY id;
SELECT VALUE n FROM c WITH INDEX by_n WHERE n >= 2990 ORDER BY n;
CREATE |c:2000..2004| RETURN id;
CREATE |c:3000..3003| RETURN AFTER;
CREATE |c:4000..4003| RETURN VALUE n;
CREATE ONLY |c:5000..5001|;
