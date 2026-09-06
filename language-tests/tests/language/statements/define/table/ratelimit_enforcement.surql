/**
[env]
# Consumed rate-limit tokens persist as bucket state in the KV store.
clean = true

[test]
reason = "Table-level RATELIMIT denies once the bucket is exhausted; the denied write is rolled back"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
error = "The query exceeded the rate limit for table post"

[[test.results]]
value = "[{ count: 1 }]"
*/

DEFINE TABLE post RATELIMIT FOR CREATE BY 'shared-key' LIMIT 1 PER 1h;
CREATE post:1;
CREATE post:2;
SELECT count() FROM post GROUP ALL;
