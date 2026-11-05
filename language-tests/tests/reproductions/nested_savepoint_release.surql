/**
[env]
namespace = true
database = true

[test]
reason = """
Releasing a nested savepoint must keep its writes undoable by the enclosing
savepoint. The data of an UPSERT is computed inside the savepoint its create
attempt takes, and a write in that data -- a nested UPSERT -- takes and settles
a savepoint of its own while the outer one is still open. When the target
record already exists, the create attempt fails and is rolled back to its
savepoint, which has to undo everything the attempt wrote, the nested write
included; the statement then runs as an update and writes it once. A counter
that reads one after the update, not two, shows the attempt's write was undone.
The UPSERT of a record that does not exist shows the nested write does stand
when the attempt stands.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: thing:0, k: 1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ hits: 1, id: audit:1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ hits: 1, id: audit:1 }, { hits: 1, id: audit:2 }]"
*/

DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE thing SCHEMALESS;
CREATE thing:0 SET k = 1;
UPSERT thing:0 SET w = (UPSERT audit:1 SET hits = (hits ?? 0) + 1 RETURN VALUE hits)[0] RETURN VALUE w;
SELECT * FROM audit;
UPSERT thing:1 SET w = (UPSERT audit:2 SET hits = (hits ?? 0) + 1 RETURN VALUE hits)[0] RETURN VALUE w;
SELECT * FROM audit ORDER BY id;
