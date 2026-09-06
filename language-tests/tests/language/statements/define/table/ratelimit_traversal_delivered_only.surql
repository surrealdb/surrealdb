/**
[env]
# Consumed rate-limit tokens persist as bucket state in the KV store.
clean = true

[test]
reason = "Traversal charges only tables whose record data is delivered: ->friend->user.* returns user data (charged per delivered user record plus the source row) but no friend data, so the friend policy is never charged; the friend limit of 1 would otherwise deny the second query"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:drew }]"

[[test.results]]
value = "[{ id: user:chiru }]"

[[test.results]]
value = "[{ id: friend:0rna2uysjc0khu65qsk6, in: user:drew, out: user:chiru }]"
skip-record-id-key = true

[[test.results]]
value = "[{ friends: [{ id: user:chiru }] }]"

[[test.results]]
value = "[{ friends: [{ id: user:chiru }] }]"

[[test.results]]
error = "The query exceeded the rate limit for table user"
*/

DEFINE TABLE user RATELIMIT FOR SELECT BY 'k' LIMIT 4 PER 1h;
DEFINE TABLE friend RATELIMIT FOR SELECT BY 'k' LIMIT 1 PER 1h;
CREATE user:drew;
CREATE user:chiru;
RELATE user:drew->friend->user:chiru;
SELECT ->friend->user.* AS friends FROM user:drew;
SELECT ->friend->user.* AS friends FROM user:drew;
SELECT ->friend->user.* AS friends FROM user:drew;
