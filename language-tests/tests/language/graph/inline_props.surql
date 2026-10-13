/**
[test]
reason = "Inline edge properties: filtered traversals over INLINE fields answer from the adjacency payloads and stay byte-identical to record-path semantics across writes, updates, NONE/NULL values, over-cap spills, and schema mutations that stale the payloads"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "[likes:l2, likes:l3]"

[[test.results]]
value = "[person:p2, person:p3]"

[[test.results]]
value = "[likes:l4]"

[[test.results]]
value = "[likes:l1]"

[[test.results]]
value = "[likes:l2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:l1, likes:l2, likes:l3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:l1, likes:l2, likes:l3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:l3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[likes:l1, likes:l2, likes:l3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:l1, likes:l2, likes:l3]"
*/

DEFINE TABLE person;
DEFINE TABLE likes TYPE RELATION;
DEFINE FIELD score ON likes TYPE number INLINE;
DEFINE FIELD tag ON likes TYPE option<string> INLINE;
CREATE person:p0, person:p1, person:p2, person:p3 RETURN NONE;
RELATE person:p0->likes:l1->person:p1 SET score = 3, tag = 'a' RETURN NONE;
RELATE person:p0->likes:l2->person:p2 SET score = 7 RETURN NONE;
RELATE person:p0->likes:l3->person:p3 SET score = 9, tag = 'b' RETURN NONE;
RELATE person:p1->likes:l4->person:p2 SET score = 5, tag = 'a' RETURN NONE;

-- Payload-served predicates, every direction.
SELECT VALUE ->(likes WHERE score > 5) FROM ONLY person:p0;
SELECT VALUE ->(likes WHERE score > 5)->person FROM ONLY person:p0;
SELECT VALUE <-(likes WHERE score <= 5) FROM ONLY person:p2;
SELECT VALUE ->(likes WHERE tag = 'a') FROM ONLY person:p0;
-- An absent optional value is NONE in the payload, exactly as on the record.
SELECT VALUE ->(likes WHERE tag = NONE) FROM ONLY person:p0;

-- Updates restamp the payloads in the same transaction.
UPDATE likes:l1 SET score = 10 RETURN NONE;
SELECT VALUE ->(likes WHERE score > 5) FROM ONLY person:p0;

-- A payload past the cap spills; the edge answers from its record.
UPDATE likes:l2 SET tag = string::repeat('x', 100) RETURN NONE;
SELECT VALUE ->(likes WHERE score > 5) FROM ONLY person:p0;

-- A predicate touching a non-inline field takes the record path wholesale.
UPDATE likes:l3 SET note = 'keep' RETURN NONE;
SELECT VALUE ->(likes WHERE score > 5 AND note = 'keep') FROM ONLY person:p0;

-- Changing the inline set stales every payload: results never change, and
-- the next write repairs the written edge.
DEFINE FIELD weight ON likes TYPE option<number> INLINE;
SELECT VALUE ->(likes WHERE score > 5) FROM ONLY person:p0;
UPDATE likes:l1 SET weight = 1 RETURN NONE;
SELECT VALUE ->(likes WHERE score > 5) FROM ONLY person:p0;
