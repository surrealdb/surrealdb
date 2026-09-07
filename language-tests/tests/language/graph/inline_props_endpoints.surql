/**
[test]
reason = "An edge's endpoints are fixed at RELATE time: on a table with INLINE fields an update-shaped write can never move `in`/`out` — a user payload is reset to the committed endpoints, and a schema VALUE clause that lands different ones is rejected — so the adjacency rewrite can never mint pointer keys for endpoints the edge was not related between"

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
value = "[{ id: likes:l1, in: person:p0, out: person:p1, score: 5 }]"

[[test.results]]
value = "[person:p1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:l1]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found person:p9 for the `out` field, which does not match the existing field value"

[[test.results]]
value = "[person:p1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[likes:l1]"
*/

DEFINE TABLE person;
DEFINE TABLE likes TYPE RELATION;
DEFINE FIELD score ON likes TYPE number INLINE;
CREATE person:p0, person:p1, person:p9 RETURN NONE;
RELATE person:p0->likes:l1->person:p1 SET score = 3 RETURN NONE;

-- A user payload cannot move the committed endpoints: `out` is reset to
-- the value RELATE stored, the rest of the update applies, and no
-- phantom edge appears at the attempted target.
UPDATE likes:l1 SET out = person:p9, score = 5;
SELECT VALUE ->likes->person FROM ONLY person:p0;
SELECT VALUE <-likes FROM ONLY person:p9;
SELECT VALUE ->(likes WHERE score = 5) FROM ONLY person:p0;

-- A schema clause runs after that reset, so it could still move the
-- field: a VALUE clause landing a different `out` rejects every update
-- of the edge instead of silently rewriting its adjacency.
DEFINE FIELD OVERWRITE out ON likes TYPE record<person> VALUE person:p9;
UPDATE likes:l1 SET score = 7;
SELECT VALUE ->likes->person FROM ONLY person:p0;
SELECT VALUE <-likes FROM ONLY person:p9;
SELECT VALUE ->(likes WHERE score = 5) FROM ONLY person:p0;
