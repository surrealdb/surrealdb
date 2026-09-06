/**
[env]
# Consumed rate-limit tokens persist as bucket state in the KV store.
clean = true

[test]
reason = "When traversal delivers edge data (->friend.{kind}), the edge table's policy is charged and denial is atomic: the friend limit of 1 admits the first query and denies the whole second query — no mixed responses where some records carry edge data and others do not"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:drew }]"

[[test.results]]
value = "[{ id: user:chiru }]"

[[test.results]]
value = "[{ id: friend:ql1strtr5afc61by7qm1, in: user:drew, kind: 'best', out: user:chiru }]"
skip-record-id-key = true

[[test.results]]
value = "[{ friends: [{ kind: 'best' }] }]"

[[test.results]]
error = "The query exceeded the rate limit for table friend"
*/

DEFINE TABLE user RATELIMIT FOR SELECT BY 'k' LIMIT 100 PER 1h;
DEFINE TABLE friend RATELIMIT FOR SELECT BY 'k' LIMIT 1 PER 1h;
CREATE user:drew;
CREATE user:chiru;
RELATE user:drew->friend->user:chiru SET kind = 'best';
SELECT ->friend.{kind} AS friends FROM user:drew;
SELECT ->friend.{kind} AS friends FROM user:drew;
