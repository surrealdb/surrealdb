/**
[env]
# Consumed rate-limit tokens persist as bucket state in the KV store.
clean = true

[test]
reason = "Rate limits meter delivered records: a filter matching nothing charges nothing, while delivering more records than the bucket capacity denies the whole statement — never a partial response"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
value = "[{ id: post:2 }]"

[[test.results]]
value = "[{ id: post:3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "The query exceeded the rate limit for table post"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
error = "The query exceeded the rate limit for table post"
*/

DEFINE TABLE post RATELIMIT FOR SELECT BY 'shared-key' LIMIT 2 PER 1h;
CREATE post:1;
CREATE post:2;
CREATE post:3;
SELECT * FROM post WHERE title = 'no-such-row';
SELECT * FROM post WHERE title = 'no-such-row';
SELECT * FROM post;
SELECT * FROM post WHERE id = post:1;
SELECT * FROM post WHERE id = post:1;
SELECT * FROM post WHERE id = post:1;
