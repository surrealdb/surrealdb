/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 60000

[test]
reason = "LIMIT 1 over a record range of 100,000 records whose computed field fails on the last reads up to its limit and answers, rather than reading and judging the whole range"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, c: 1, id: t:1 }]"

[[test.results]]
value = "[t:99998, t:99999]"

[[test.results]]
value = "[]"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD c ON t COMPUTED IF id = t:100000 { <int> 'x' } ELSE { a };
CREATE |t:1..=100000| SET a = 1 RETURN NONE;
SELECT * FROM t:1.. LIMIT 1;
SELECT VALUE id FROM t:99998.. LIMIT 2;
SELECT * FROM t:1.. LIMIT 0;
