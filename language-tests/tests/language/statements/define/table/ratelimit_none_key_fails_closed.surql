/**
[test]
reason = "A RATELIMIT BY key that evaluates to NONE denies the statement (fail closed) instead of sharing one bucket"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
error = "The rate limit key for table post evaluated to NONE. Guard the policy with a WHERE clause or use a key that is always present"
*/

DEFINE TABLE post RATELIMIT FOR SELECT BY $auth.id LIMIT 100 PER 1h;
CREATE post:1;
SELECT * FROM post;
