/**
[env]
namespace = true
database = true

[test]
upgrade = true
introduced = ">=3.1.0-beta.3"
reason = """
The worked example of a feature that does not exist at the head of the chain.
DISKANN arrived in 3.1.0-beta.3, so the walk carries an empty store through
every earlier release, creates the index on the first version that supports it,
and from there every later release inserts into the graph its predecessor left
behind and re-runs the same KNN query.

`introduced` is where "add the feature at the version that introduced it" is
expressed — the same field the filename prefix mirrors — and the walk takes it
as its default `since`; there is nothing to configure per release after that.

No `[[test.repair]]`: the walk is deliberately routed around the 3.3.0 betas,
which reject a pre-shared-doc-ID graph and would have the walk read a rebuilt
structure from that point on rather than the one 3.1.0 built. That rejection is
covered pairwise by upgrade/define/index_diskann.surql."""

[test.chain]
setup = "upgrade/chain/3.01.00_index_diskann_setup.surql"
mutate = "upgrade/chain/3.01.00_index_diskann_mutate.surql"

[[test.results]]
match = '''string::starts_with($result.indexes.chain_dann, "DEFINE INDEX chain_dann ON chain_pts FIELDS point DISKANN DIMENSION 4 DIST EUCLIDEAN TYPE F32")'''

[[test.results]]
value = "NONE"

[[test.results]]
match = '''count($result) == $hop'''

[[test.results]]
match = '''$result[0].hop == 1'''

*/
INFO FOR TABLE chain_pts;
LET $probe = [0.0, 0.0, 0.0, 0.0];
-- Every vector any earlier version wrote is still reachable through the graph.
SELECT id FROM chain_pts WHERE point <|100,100|> $probe;
-- The first version's vector sits closest to the origin, so a graph that has
-- been maintained correctly across the whole walk still returns it first.
SELECT hop FROM chain_pts WHERE point <|1,100|> $probe;
