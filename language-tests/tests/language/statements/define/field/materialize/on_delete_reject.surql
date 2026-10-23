/**
[env]
namespace = true
database = true

[test]
reason = """
	Deleting a referenced record follows the ON DELETE mode of the referencing
	field and the clause adds nothing to it: REJECT refuses the delete and leaves every list as it was. A second
	referenced record keeps its own list through the delete.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:keep, kids: [] }, { id: p:gone, kids: [] }]"

[[test.results]]
value = "[{ id: k:1, p: p:gone }]"

[[test.results]]
value = "[{ id: k:2, p: p:keep }]"

[[test.results]]
error = "Cannot delete `p:gone` as it is referenced by `k:1` with an ON DELETE REJECT clause"

[[test.results]]
value = "[[k:1, p:gone], [k:2, p:keep]]"

[[test.results]]
value = "[[p:gone, [k:1]], [p:keep, [k:2]]]"
*/

DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD kids ON p TYPE array<record<k>> DEFAULT [] READONLY;
DEFINE TABLE k SCHEMAFULL;
DEFINE FIELD p ON k TYPE option<record<p>> REFERENCE ON DELETE REJECT MATERIALIZE INTO p.kids;
CREATE p:keep, p:gone;
CREATE k:1 SET p = p:gone;
CREATE k:2 SET p = p:keep;
DELETE p:gone;
SELECT VALUE [id, p] FROM k ORDER BY id;
SELECT VALUE [id, kids] FROM p ORDER BY id;
