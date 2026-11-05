/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Walks one datastore through every version in the chain. The index is defined by
the first version, and each later version inserts through it and then reads back
every row any earlier version wrote.

This is the property adjacent-pair testing cannot express: not that one release
can read the release before it, but that a structure created once stays correct
while every release in turn maintains it."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_index_standard_setup.surql"
mutate = "upgrade/chain/3.00.00_index_standard_mutate.surql"

[[test.results]]
match = '''$result.indexes.chain_std_org == "DEFINE INDEX chain_std_org ON chain_std FIELDS org"'''

[[test.results]]
match = '''count($result) == $hop'''

[[test.results]]
match = '''$result[0].count == $hop'''

[[test.results]]
match = '''count(array::distinct($result)) == $hop'''


*/
INFO FOR TABLE chain_std;
-- Every row every earlier version wrote is still there and still indexed.
SELECT id FROM chain_std WITH INDEX chain_std_org WHERE org = 'surrealdb' ORDER BY id;
SELECT count() FROM chain_std WITH INDEX chain_std_org WHERE org = 'surrealdb' GROUP ALL;
-- One row per version walked so far, each stamped by the binary that wrote it:
-- proof that every release in turn maintained the index, not just the last one.
SELECT VALUE wrote_at FROM chain_std;
