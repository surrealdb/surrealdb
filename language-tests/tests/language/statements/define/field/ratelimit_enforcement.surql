/**
[env]
# Consumed rate-limit tokens persist as bucket state in the KV store.
clean = true

[test]
reason = "Field-level RATELIMIT denies once the bucket is exhausted; the denied write is rolled back"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: post:1, name: 'one' }]"

[[test.results]]
error = "The query exceeded the rate limit for field name on post"

[[test.results]]
value = "[{ count: 1 }]"
*/

DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD name ON post TYPE string RATELIMIT FOR CREATE BY 'shared-key' LIMIT 1 PER 1h;
CREATE post:1 SET name = 'one';
CREATE post:2 SET name = 'two';
SELECT count() FROM post GROUP ALL;
