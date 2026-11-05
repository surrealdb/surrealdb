/**
[test]
reason = "A backward lightweight scan (ORDER BY id DESC) streams the exact reverse of the forward scan, across IN tables and within each table, and composes with START and LIMIT"

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
value = "[]"

[[test.results]]
value = "[likes:[org:o1, person:x], likes:[org:o2, person:y], likes:[person:x, person:y], likes:[person:y, person:x]]"

[[test.results]]
value = "[likes:[person:y, person:x], likes:[person:x, person:y], likes:[org:o2, person:y], likes:[org:o1, person:x]]"

[[test.results]]
value = "[likes:[person:y, person:x], likes:[person:x, person:y]]"

[[test.results]]
value = "[likes:[person:x, person:y], likes:[org:o2, person:y]]"

[[test.results]]
value = "[likes:[org:o1, person:x]]"
*/

DEFINE TABLE person;
DEFINE TABLE org;
DEFINE TABLE likes TYPE RELATION IN person | org OUT person LIGHTWEIGHT;
CREATE person:x, person:y RETURN NONE;
CREATE org:o1, org:o2 RETURN NONE;

RELATE org:o1->likes->person:x RETURN NONE;
RELATE org:o2->likes->person:y RETURN NONE;
RELATE person:x->likes->person:y RETURN NONE;
RELATE person:y->likes->person:x RETURN NONE;

-- Forward: ascending canonical-id order, IN tables in sorted order.
SELECT VALUE id FROM likes;
-- Backward: the exact reverse, including across the two IN tables.
SELECT VALUE id FROM likes ORDER BY id DESC;
SELECT VALUE id FROM likes ORDER BY id DESC LIMIT 2;
-- START skips within the reversed stream.
SELECT VALUE id FROM likes ORDER BY id DESC START 1 LIMIT 2;
SELECT VALUE id FROM likes ORDER BY id DESC START 3;
