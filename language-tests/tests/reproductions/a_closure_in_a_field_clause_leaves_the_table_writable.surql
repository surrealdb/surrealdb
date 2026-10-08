/**
[env]
namespace = true
database = true

[test]
reason = "A closure in a field's VALUE, ASSERT, DEFAULT, COMPUTED or PERMISSIONS clause is evaluated by every write to the table: CREATE, UPDATE, UPSERT, INSERT and DELETE run, and an ASSERT closure refuses a value it rejects."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: ['b'], id: f:1, n: [2], tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ c: ['b', 'c'], id: f:1, n: [2], tags: ['a', 'b', 'c'] }]"

[[test.results]]
value = "[{ c: ['x'], id: f:2, n: [2], tags: ['x'] }]"

[[test.results]]
value = "[{ c: ['q'], id: f:3, n: [5], tags: ['q'] }]"

[[test.results]]
error = "Found [0] for field `n`, with record `f:1`, but field must conform to: $value.all(|$x: any| $x > 0)"

[[test.results]]
value = "[{ c: ['b', 'c'], id: f:1, n: [2], tags: ['a', 'b', 'c'] }, { c: ['x'], id: f:2, n: [2], tags: ['x'] }, { c: ['q'], id: f:3, n: [5], tags: ['q'] }]"

[[test.results]]
value = "[{ c: ['b', 'c'], id: f:1, n: [2], tags: ['a', 'b', 'c'] }]"

[[test.results]]
value = "[f:2, f:3]"

*/

DEFINE FIELD tags ON f VALUE $value.map(|$t| string::lowercase($t));
DEFINE FIELD n ON f ASSERT $value.all(|$x| $x > 0) DEFAULT [1].map(|$x| $x + 1);
DEFINE FIELD c ON f COMPUTED tags.filter(|$t| $t != 'a');
DEFINE FIELD p ON f TYPE option<int> PERMISSIONS FOR select WHERE [1, 2].any(|$x| $x = 1);
CREATE f:1 SET tags = ['A', 'B'];
UPDATE f:1 SET tags += 'C';
UPSERT f:2 SET tags = ['X'];
INSERT INTO f { id: f:3, tags: ['Q'], n: [5] };
UPDATE f:1 SET n = [0];
SELECT * FROM f ORDER BY id;
DELETE f:1 RETURN BEFORE;
SELECT VALUE id FROM f ORDER BY id;
