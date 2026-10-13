/**
[test]
reason = "Graph traversals over tables with an INLINE EDGES cap: every output mode answers identically to an uncached table, across cache materialisation, per-entry maintenance, spill past the cap, delete-to-empty, and a vertex-delete cascade"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:l1, likes:l2, likes:l3]"

[[test.results]]
value = "[likes:l4]"

[[test.results]]
value = "[person:p2]"

[[test.results]]
value = "[likes:l2, likes:l4]"

[[test.results]]
value = "[likes:l1, likes:l4]"

[[test.results]]
value = "[{ id: likes:l4, in: person:p1, out: person:p2 }]"

[[test.results]]
value = "[likes:l4]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:l1, likes:l3]"

[[test.results]]
value = "[likes:l3]"
*/

-- A small cap so person:p0 (three edges) spills while person:p1 stays cached.
DEFINE TABLE person INLINE EDGES 2;
DEFINE TABLE likes TYPE RELATION;
CREATE person:p0, person:p1, person:p2, person:p3 RETURN NONE;
RELATE person:p0->likes:l1->person:p1 RETURN NONE;
RELATE person:p0->likes:l2->person:p2 RETURN NONE;
RELATE person:p0->likes:l3->person:p3 RETURN NONE;
RELATE person:p1->likes:l4->person:p2 RETURN NONE;

-- Spilled source: served by the scan, still complete.
SELECT VALUE ->likes FROM ONLY person:p0;
-- Cached source, every output mode.
SELECT VALUE ->likes FROM ONLY person:p1;
SELECT VALUE ->likes->person FROM ONLY person:p1;
SELECT VALUE <-likes FROM ONLY person:p2;
SELECT VALUE <->likes FROM ONLY person:p1;
SELECT VALUE ->likes.* FROM ONLY person:p1;
SELECT VALUE ->(likes WHERE out = person:p2) FROM ONLY person:p1;

-- Deleting the cached source's only edge leaves an authoritative empty
-- cache: the traversal answers "none" without a scan.
DELETE likes:l4;
SELECT VALUE ->likes FROM ONLY person:p1;

-- A vertex-delete cascades over the edges touching it, maintaining the
-- caches of every surviving endpoint.
DELETE person:p2;
SELECT VALUE ->likes FROM ONLY person:p0;
SELECT VALUE <-likes FROM ONLY person:p3;
