/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A field's VALUE clause and its PERMISSIONS predicate that call a closure are evaluated on every write to the table"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, tags: ['a', 'b'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t2:1, nums: [1] }]"

[[test.results]]
value = "[{ id: t2:1, nums: [2] }]"
*/
DEFINE FIELD tags ON t VALUE $value.map(|$v| string::lowercase($v));
CREATE t:1 SET tags = ['A', 'B'];
DEFINE FIELD nums ON t2 PERMISSIONS FOR update WHERE $value.all(|$x| $x > 0);
CREATE t2:1 SET nums = [1];
UPDATE t2:1 SET nums = [2];
