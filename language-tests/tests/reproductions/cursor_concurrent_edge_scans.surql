/**
[env]
namespace = true
database = true

[test]
reason = "Regression: a SELECT->edge FROM table walk with more outer rows than the (previous) iterator cache capacity must return correct edge sets per row. The pre-cursor design held a per-transaction LRU of read-only iterators with capacity 4; at the 5th outer row the outer-table iterator would be evicted (a correctness no-op but a steep perf cliff). The cursor design owns the iterator per logical scan, so N outer rows ⇒ N+1 live cursors (one outer + one per row's edge prefix as it's visited) and no eviction. This test asserts the resulting edges are correct for 8 outer persons each with a distinct ->knows->person edge."

[[test.results]]
value = "[{ id: person:p0 }, { id: person:p1 }, { id: person:p2 }, { id: person:p3 }, { id: person:p4 }, { id: person:p5 }, { id: person:p6 }, { id: person:p7 }]"

[[test.results]]
value = "[{ id: knows:k0, in: person:p0, out: person:p1 }]"

[[test.results]]
value = "[{ id: knows:k1, in: person:p1, out: person:p2 }]"

[[test.results]]
value = "[{ id: knows:k2, in: person:p2, out: person:p3 }]"

[[test.results]]
value = "[{ id: knows:k3, in: person:p3, out: person:p4 }]"

[[test.results]]
value = "[{ id: knows:k4, in: person:p4, out: person:p5 }]"

[[test.results]]
value = "[{ id: knows:k5, in: person:p5, out: person:p6 }]"

[[test.results]]
value = "[{ id: knows:k6, in: person:p6, out: person:p7 }]"

[[test.results]]
value = "[{ id: knows:k7, in: person:p7, out: person:p0 }]"

[[test.results]]
value = "[{ friends: [person:p1], id: person:p0 }, { friends: [person:p2], id: person:p1 }, { friends: [person:p3], id: person:p2 }, { friends: [person:p4], id: person:p3 }, { friends: [person:p5], id: person:p4 }, { friends: [person:p6], id: person:p5 }, { friends: [person:p7], id: person:p6 }, { friends: [person:p0], id: person:p7 }]"
*/

-- Eight outer rows, each with exactly one outgoing knows edge. With 8
-- outer rows and the inner edge scan opening one cursor per row, the
-- system must hold the outer person cursor live while up to 8 edge
-- cursors come and go.
CREATE person:p0, person:p1, person:p2, person:p3, person:p4, person:p5, person:p6, person:p7;
RELATE person:p0->knows:k0->person:p1;
RELATE person:p1->knows:k1->person:p2;
RELATE person:p2->knows:k2->person:p3;
RELATE person:p3->knows:k3->person:p4;
RELATE person:p4->knows:k4->person:p5;
RELATE person:p5->knows:k5->person:p6;
RELATE person:p6->knows:k6->person:p7;
RELATE person:p7->knows:k7->person:p0;

-- The exact case the user called out in PR 179's follow-up: an outer
-- table walk plus one inner edge scan per row. Every row must see its
-- own friend; no row should see another row's edges (which would happen
-- if the cursor was evicted and a later seek positioned the iterator
-- somewhere unexpected).
SELECT id, ->knows->person AS friends FROM person ORDER BY id;
