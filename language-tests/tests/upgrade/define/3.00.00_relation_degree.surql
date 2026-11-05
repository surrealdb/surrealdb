/**
[env]
imports = ["upgrade/define/3.00.00_relation_degree_import.surql"]

[test]
upgrade = true
reason = """
`count(->edge)` is answered straight out of the adjacency band rather than by
materializing the neighbour list, which makes it a second, independent reader
over the exact keys every released binary's RELATE wrote. Every other relation
test in the suite resolves neighbours instead, so a counting reader that
mis-reads an older per-edge layout leaves all of them green.

Counts and resolves are therefore both asserted to literal numbers, never
against each other. A count that merely agrees with a resolve over the same
keys proves nothing when the two share a fault, which is exactly the case the
deletions below construct.

Those deletions are the load-bearing half. Retracting an edge has to remove
every adjacency key that edge contributed, at both of its endpoints, and which
keys those are is decided from what the edge record itself carries. The
released binaries in the matrix do not all stamp an edge the same way, so a
reader that mis-classifies an old edge retracts a key that was never written
and leaves the real one in place. That survivor is invisible to a neighbour
walk, which silently drops a key pointing at a record that no longer exists,
and invisible to a count-versus-length self-check, which reads the same
survivor down both sides. Only the literal post-deletion degrees can see it,
and only the per-endpoint assertions distinguish a retraction that missed the
hub's side of the edge from one that missed the target's.

The bidirectional count is asserted separately because it sums two scopes the
writing binary laid down independently: it can be wrong while both single
directions are right.

The seed axis is what this file is for. It sends 3.0.x, 3.1.x and 3.2.x writers
straight to the current checkout, so the counting reader is driven over both
released adjacency layouts with no intermediate binary in between to rewrite
either of them; an adjacent-pair run exercises the same assertions but only
ever shows the release directly after the writer.

Only query results are asserted here. A degree read is a read like any other
and can leave the hub's adjacency repacked behind it, so no assertion may
depend on the storage shape. The import's fan-out is sized below the fold
threshold for a different reason: it keeps the adjacency in the layout the
writing binary produced, which is the layout this test exists to read."""

[[test.results]]
value = "8"

[[test.results]]
value = "3"

[[test.results]]
value = "11"

[[test.results]]
value = "8"

[[test.results]]
value = "3"

[[test.results]]
value = "0"

[[test.results]]
value = "0"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "7"

[[test.results]]
value = "2"

[[test.results]]
value = "9"

[[test.results]]
value = "7"

[[test.results]]
value = "[up_degree_person:t1, up_degree_person:t3, up_degree_person:t4, up_degree_person:t5, up_degree_person:t6, up_degree_person:t7, up_degree_person:t8]"

[[test.results]]
value = "[up_degree_person:t1, up_degree_person:t2]"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

*/
-- Outbound degree of the hub, counted out of the adjacency the old binary
-- wrote. Eight edges, asserted as a literal.
SELECT VALUE count(->up_degree_knows) FROM ONLY up_degree_person:hub;

-- Inbound degree. Deliberately different from the outbound one, so a reader
-- that counts the wrong direction's scope cannot pass by coincidence.
SELECT VALUE count(<-up_degree_knows) FROM ONLY up_degree_person:hub;

-- Both directions summed: two separately written scopes, one answer.
SELECT VALUE count(<->up_degree_knows) FROM ONLY up_degree_person:hub;

-- The resolving cross-check. Counting keys that resolve to nothing would agree
-- with the count above and disagree with this.
SELECT VALUE array::len(->up_degree_knows->up_degree_person) FROM ONLY up_degree_person:hub;
SELECT VALUE array::len(<-up_degree_knows<-up_degree_person) FROM ONLY up_degree_person:hub;

-- A vertex the old binary never related to anything: an absent adjacency scope
-- counts as zero rather than erroring.
SELECT VALUE count(->up_degree_knows) FROM ONLY up_degree_person:lone;
SELECT VALUE count(<->up_degree_knows) FROM ONLY up_degree_person:lone;

-- Retract one edge from each direction of the hub's adjacency. Both were
-- written by the old binary, so the retraction has to work out which keys that
-- binary laid down from the edge record alone.
DELETE up_degree_knows:o2;
DELETE up_degree_knows:i3;

-- The degrees that must remain. A retraction that removed a key which was
-- never written leaves the real one behind and these read one too high.
SELECT VALUE count(->up_degree_knows) FROM ONLY up_degree_person:hub;
SELECT VALUE count(<-up_degree_knows) FROM ONLY up_degree_person:hub;
SELECT VALUE count(<->up_degree_knows) FROM ONLY up_degree_person:hub;

-- The resolving readers agree on the same post-deletion cardinality...
SELECT VALUE array::len(->up_degree_knows->up_degree_person) FROM ONLY up_degree_person:hub;

-- ...and on which neighbours specifically survived, so a count that is right
-- for the wrong reason still fails here.
SELECT VALUE array::sort(->up_degree_knows->up_degree_person) FROM ONLY up_degree_person:hub;
SELECT VALUE array::sort(<-up_degree_knows<-up_degree_person) FROM ONLY up_degree_person:hub;

-- The far endpoint of each deleted edge. An edge contributes a key at both of
-- its vertices, and these are the only assertions that see the target's side:
-- t2 loses its inbound key and keeps its outbound one...
SELECT VALUE count(<-up_degree_knows) FROM ONLY up_degree_person:t2;
SELECT VALUE count(->up_degree_knows) FROM ONLY up_degree_person:t2;

-- ...and t3 loses its outbound key and keeps its inbound one.
SELECT VALUE count(->up_degree_knows) FROM ONLY up_degree_person:t3;
SELECT VALUE count(<-up_degree_knows) FROM ONLY up_degree_person:t3;
