/**
[test]
reason = "A falsy RATELIMIT WHERE condition exempts the statement from the policy"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
value = "[{ id: post:1 }]"

[[test.results]]
value = "[{ id: post:1 }]"
*/

DEFINE TABLE post RATELIMIT FOR SELECT WHERE false BY 'shared-key' LIMIT 1 PER 1h;
CREATE post:1;
SELECT * FROM post;
SELECT * FROM post;
