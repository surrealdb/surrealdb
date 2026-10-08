/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A graph lookup written as a user-defined function's argument, and a filtered lookup in a COMPUTED field, are read where they stand; an element filter whose WHERE holds a subquery reading no row answers in a WHERE clause"

[[test.results]]
value = "[{ id: user:1 }, { id: user:2 }, { id: post:1 }]"

[[test.results]]
value = "[{ id: likes:1, in: user:1, out: post:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:1, n: 1 }, { id: user:2, n: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ af: [post:1], id: user:1 }, { af: [], id: user:2 }]"

[[test.results]]
value = "[{ id: tag:a, name: 'a' }]"

[[test.results]]
value = "[{ id: post:2, tags: ['a', 'b'] }]"

[[test.results]]
value = "[post:2]"
*/
CREATE user:1, user:2, post:1;
RELATE user:1->likes:1->post:1;
DEFINE FUNCTION fn::n($x: any) { RETURN count($x) };
SELECT id, fn::n(->likes->post) AS n FROM user ORDER BY id;
DEFINE FUNCTION fn::rank($i: record) { RETURN record::id($i) };
SELECT VALUE fn::rank(->likes->post[0]) FROM user:1;
DEFINE FIELD af ON user COMPUTED ->likes->(post WHERE id = post:1).id;
SELECT id, af FROM user ORDER BY id;
CREATE tag:a SET name = 'a';
CREATE post:2 SET tags = ['a', 'b'];
SELECT VALUE id FROM post WHERE tags[WHERE $this IN (SELECT VALUE name FROM tag)];
