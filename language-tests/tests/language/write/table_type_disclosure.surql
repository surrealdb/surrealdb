/**
[env]
imports = ["language/write/table_type_disclosure_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "SECURITY: pins how much of a table's type each mutation discloses to an actor whose creates are denied. CREATE gates on permission before it type-checks, so the actor is skipped in silence. UPSERT, INSERT and RELATE type-check first and name the table's type in the error — with two escapes that are equally part of the contract: UPSERT's error is swallowed when its fallback update is denied too, and RELATE's gate does fire, and silence the error, once the statement names the edge id itself. An operator reads a table's type out of a refusal exactly where this test says so."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Found record: `edge_updatable:1` which is not a relation, but expected a RELATION IN person OUT person"

[[test.results]]
error = "Found record: `edge:1` which is not a relation, but expected a RELATION IN person OUT person"

[[test.results]]
# The generated edge id is not reproducible, so only the refusal is pinned.
error = true

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

-- CREATE gates on permission first: a denied actor is skipped, and learns
-- nothing about the table refusing normal records.
CREATE edge:1 SET x = 1;

-- UPSERT type-checks first, but reports the failure only if the fallback
-- update fails as well — a denied update leaves the statement silent.
UPSERT edge:1 SET x = 1;
UPSERT edge_updatable:1 SET x = 1;

-- INSERT type-checks first and reports immediately.
INSERT INTO edge { id: edge:1, x: 1 };

-- RELATE's create gate only judges a document that already has a record id. An
-- edge whose id the statement leaves to be generated has none when the gate
-- runs, so the gate passes and the type check answers; naming the edge id makes
-- the gate fire and the refusal silent.
RELATE person:bob->plain->person:bob;
RELATE person:bob->plain:e1->person:bob;

-- With the type check satisfied, the gate is the only thing left, and every
-- statement is silent again.
CREATE plain:1 SET x = 1;
UPSERT plain:1 SET x = 1;
INSERT INTO plain { id: plain:1, x: 1 };
