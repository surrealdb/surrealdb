/**
[test]
reason = "START on a lightweight relation skips each edge exactly once: the record-less scan defers all skipping to the collector, so START, START + LIMIT, and a START past the end of the relation return the same rows a classic relation would"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[follows:[person:p1, person:p2], follows:[person:p1, person:p3], follows:[person:p2, person:p3], follows:[person:p2, person:p4], follows:[person:p3, person:p4], follows:[person:p4, person:p1]]"

[[test.results]]
value = "[follows:[person:p2, person:p3], follows:[person:p2, person:p4], follows:[person:p3, person:p4], follows:[person:p4, person:p1]]"

[[test.results]]
value = "[follows:[person:p2, person:p3], follows:[person:p2, person:p4]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[follows:[person:p3, person:p4], follows:[person:p4, person:p1]]"
*/

DEFINE TABLE person;
DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
CREATE person:p1, person:p2, person:p3, person:p4 RETURN NONE;

RELATE person:p1->follows->person:p2 RETURN NONE;
RELATE person:p1->follows->person:p3 RETURN NONE;
RELATE person:p2->follows->person:p3 RETURN NONE;
RELATE person:p2->follows->person:p4 RETURN NONE;
RELATE person:p3->follows->person:p4 RETURN NONE;
RELATE person:p4->follows->person:p1 RETURN NONE;

-- The record-less scan emits in ascending canonical-id order.
SELECT VALUE id FROM follows;
-- START drops exactly that many edges, once.
SELECT VALUE id FROM follows START 2;
SELECT VALUE id FROM follows START 2 LIMIT 2;
-- A START past the end returns nothing.
SELECT VALUE id FROM follows START 10;
-- START and LIMIT together reach the tail.
SELECT VALUE id FROM follows START 4 LIMIT 3;
