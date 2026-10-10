/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
A B-tree index over a plain column (`FIELDS s`, no `.*`) holding a set stores
one entry per stored set, keyed by the whole set; a set is never fanned out the
way an array is. CONTAINSANY, CONTAINSALL, ANYINSIDE and ALLINSIDE over the
field ask for an element, which such a key never holds, so they return every
matching record: in SELECT, in count(), and in the WHERE of UPDATE and DELETE.
Equality with a whole set is a key the index does hold.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:1, s: {1, 2, 3} }]"

[[test.results]]
value = "[{ id: p:2, s: {3, 4} }]"

[[test.results]]
value = "[p:1, p:2]"

[[test.results]]
value = "[p:1]"

[[test.results]]
value = "[p:1, p:2]"

[[test.results]]
value = "[p:1, p:2]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[p:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[p:1, p:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/
DEFINE TABLE p SCHEMALESS;
DEFINE FIELD s ON p TYPE set<int>;
DEFINE INDEX ixp ON p FIELDS s;
CREATE p:1 SET s = <set>[1, 2, 3];
CREATE p:2 SET s = <set>[3, 4];
SELECT VALUE id FROM p WHERE s CONTAINSANY [1, 4] ORDER BY id;
SELECT VALUE id FROM p WHERE s CONTAINSALL [2, 3] ORDER BY id;
SELECT VALUE id FROM p WHERE [1, 4] ANYINSIDE s ORDER BY id;
SELECT VALUE id FROM p WHERE [3] ALLINSIDE s ORDER BY id;
SELECT count() FROM p WHERE s CONTAINSANY [1, 4] GROUP ALL;
SELECT VALUE id FROM p WHERE s = <set>[3, 4];
UPDATE p SET hit = true WHERE s CONTAINSANY [1, 4] RETURN NONE;
SELECT VALUE id FROM p WHERE hit = true ORDER BY id;
DELETE p WHERE s CONTAINSALL [3] RETURN NONE;
-- The DELETE above removed both records.
SELECT VALUE id FROM p ORDER BY id;
