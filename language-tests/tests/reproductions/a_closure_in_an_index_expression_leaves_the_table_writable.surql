/**
[env]
namespace = true
database = true

[test]
reason = "An index whose expression holds a closure is maintained by every write to its table: CREATE, UPDATE and DELETE run as they do with no such index."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: f2:1, tags: ['A'] }]"

[[test.results]]
value = "[{ id: f2:1, tags: ['B'] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/
DEFINE INDEX ci ON f2 FIELDS tags.map(|$t| string::lowercase($t));
CREATE f2:1 SET tags = ['A'];
UPDATE f2:1 SET tags = ['B'];
DELETE f2:1;
SELECT * FROM f2;
