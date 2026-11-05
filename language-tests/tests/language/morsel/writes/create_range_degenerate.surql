/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "a CREATE over a range that names no id, and a CREATE ONLY over a range that names several, answer as the serial run does, whatever width the compute half of the statement runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ id: c:6 }]"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
error = "Expected a single result output when using the ONLY keyword"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "{ id: c:200 }"

[[test.results]]
value = "[{ count: 2 }]"
*/

DEFINE TABLE c;
CREATE |c:5..5| RETURN NONE;
CREATE |c:9..5| RETURN id;
SELECT count() FROM c GROUP ALL;
CREATE |c:6..=6| RETURN id;
CREATE ONLY |c:100..103|;
SELECT count() FROM c GROUP ALL;
CREATE ONLY |c:1000..2000|;
SELECT count() FROM c GROUP ALL;
CREATE ONLY |c:200..201|;
SELECT count() FROM c GROUP ALL;
