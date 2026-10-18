/**
[env]
namespace = true
database = true
# Every statement below writes hundreds of records, which the suite's forced configuration cuts into
# a batch for each record, and RocksDB does not finish that in the default timeout while the suite
# runs in parallel.
timeout = 30000

[test]
reason = "a CREATE over a range of ids whose assert fails for two of them fails with the error of the first, and leaves what the serial run leaves, whatever width its compute half runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 300 for field `n`, with record `c:300`, but field must conform to: $value != 300 AND $value != 1500"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1199 }]"

[[test.results]]
value = "[301, 302]"

*/

DEFINE TABLE c;
DEFINE FIELD n ON c TYPE int VALUE record::id(id) ASSERT $value != 300 AND $value != 1500;
CREATE |c:1..2000| RETURN NONE;
SELECT count() FROM c GROUP ALL;
CREATE |c:301..1500| RETURN NONE;
SELECT count() FROM c GROUP ALL;
SELECT VALUE n FROM c ORDER BY id LIMIT 2;
