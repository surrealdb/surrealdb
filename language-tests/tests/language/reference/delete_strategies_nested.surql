/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A delete repairs what references it by every ON DELETE strategy, several levels deep, through records, arrays and sets"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: root:1 }, { id: root:2 }, { id: root:3 }]"

[[test.results]]
value = "[{ id: mid:1, up: root:1 }]"

[[test.results]]
value = "[{ id: mid:2, up: root:2 }]"

[[test.results]]
value = "[{ id: mid:3, up: root:3 }]"

[[test.results]]
value = "[{ id: leaf:1, up: mid:1 }]"

[[test.results]]
value = "[{ id: leaf:2, up: mid:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: side:2, noted: side:1 }]"

[[test.results]]
value = "[{ held: mid:3, id: guard:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: mid:2, up: root:2 }, { id: mid:3, up: root:3 }]"

[[test.results]]
value = "[{ id: leaf:2, up: mid:2 }]"

[[test.results]]
value = "[{ bag: [root:3], id: side:1, kept: leaf:1, note: NONE, noted: NONE, one: mid:2, roots: [root:2] }, { bag: [], id: side:2, kept: NONE, note: NONE, noted: side:1, one: NONE, roots: NONE }]"

[[test.results]]
error = "An error occurred while updating references for `root:3`: Cannot delete `mid:3` as it is referenced by `guard:1` with an ON DELETE REJECT clause"

[[test.results]]
error = "Cannot delete `mid:3` as it is referenced by `guard:1` with an ON DELETE REJECT clause"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: mid:2, up: root:2 }]"

[[test.results]]
value = "[{ bag: [], id: side:1, kept: leaf:1, note: NONE, noted: NONE, one: mid:2, roots: [root:2] }, { bag: [], id: side:2, kept: NONE, note: NONE, noted: side:1, one: NONE, roots: NONE }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ bag: [], id: side:1, kept: leaf:1, note: NONE, noted: NONE, one: NONE, roots: [] }, { bag: [], id: side:2, kept: NONE, note: NONE, noted: side:1, one: NONE, roots: NONE }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ bag: [], id: side:2, kept: NONE, note: 'gone', noted: side:1, one: NONE, roots: NONE }]"
*/

DEFINE TABLE root;
DEFINE TABLE mid;
DEFINE TABLE leaf;
DEFINE TABLE side;
DEFINE TABLE guard;
DEFINE FIELD up ON mid TYPE option<record<root>> REFERENCE ON DELETE CASCADE;
DEFINE FIELD up ON leaf TYPE option<record<mid>> REFERENCE ON DELETE CASCADE;
DEFINE FIELD roots ON side TYPE option<array<record<root>>> REFERENCE ON DELETE UNSET;
DEFINE FIELD bag ON side TYPE option<set<record<root>>> REFERENCE ON DELETE UNSET;
DEFINE FIELD one ON side TYPE option<record<mid>> REFERENCE ON DELETE UNSET;
DEFINE FIELD kept ON side TYPE option<record<leaf>> REFERENCE ON DELETE IGNORE;
DEFINE FIELD noted ON side TYPE option<record<side>> REFERENCE ON DELETE THEN (UPDATE $this SET note = 'gone');
DEFINE FIELD held ON guard TYPE option<record<mid>> REFERENCE ON DELETE REJECT;
CREATE root:1, root:2, root:3;
CREATE mid:1 SET up = root:1;
CREATE mid:2 SET up = root:2;
CREATE mid:3 SET up = root:3;
CREATE leaf:1 SET up = mid:1;
CREATE leaf:2 SET up = mid:2;
CREATE side:1 SET roots = [root:1, root:2, root:1], bag = <set> [root:1, root:3], one = mid:2, kept = leaf:1 RETURN NONE;
CREATE side:2 SET noted = side:1;
CREATE guard:1 SET held = mid:3;
DELETE root:1;
SELECT * FROM mid ORDER BY id;
SELECT * FROM leaf ORDER BY id;
SELECT id, roots, <array> (bag ?? []) AS bag, one, kept, noted, note FROM side ORDER BY id;
DELETE root:3;
DELETE mid:3;
DELETE guard:1;
DELETE root:3;
SELECT * FROM mid ORDER BY id;
SELECT id, roots, <array> (bag ?? []) AS bag, one, kept, noted, note FROM side ORDER BY id;
DELETE root;
SELECT * FROM mid ORDER BY id;
SELECT * FROM leaf ORDER BY id;
SELECT id, roots, <array> (bag ?? []) AS bag, one, kept, noted, note FROM side ORDER BY id;
DELETE side:1;
SELECT id, roots, <array> (bag ?? []) AS bag, one, kept, noted, note FROM side ORDER BY id;
