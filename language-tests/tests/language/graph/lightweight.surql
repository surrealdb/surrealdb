/**
[test]
reason = "Lightweight relation behaviour: canonical ids, synthesized records through every read shape (traversal, table scan, count, ordering, point read, FETCH-style projection), idempotent RELATE, self-loops, deletes by id and by traversal, vertex-delete cascades, and the write-shape rejections"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: follows:[person:a, person:b], in: person:a, out: person:b }]"

[[test.results]]
value = "[{ id: follows:[person:a, person:b], in: person:a, out: person:b }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: follows:[person:c, person:c], in: person:c, out: person:c }]"

[[test.results]]
value = "[follows:[person:a, person:b], follows:[person:a, person:c]]"

[[test.results]]
value = "[person:b, person:c]"

[[test.results]]
value = "[follows:[person:a, person:c], follows:[person:b, person:c], follows:[person:c, person:c]]"

[[test.results]]
value = "[follows:[person:a, person:b], follows:[person:b, person:c]]"

[[test.results]]
value = "[{ id: follows:[person:a, person:b], in: person:a, out: person:b }, { id: follows:[person:a, person:c], in: person:a, out: person:c }]"

[[test.results]]
value = "[{ id: follows:[person:a, person:b], in: person:a, out: person:b }, { id: follows:[person:a, person:c], in: person:a, out: person:c }, { id: follows:[person:b, person:c], in: person:b, out: person:c }, { id: follows:[person:c, person:c], in: person:c, out: person:c }]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[follows:[person:c, person:c], follows:[person:b, person:c]]"

[[test.results]]
value = "{ id: follows:[person:a, person:b], in: person:a, out: person:b }"

[[test.results]]
value = "person:b"

[[test.results]]
value = "true"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be given a data clause: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edge id is canonical: expected follows:[person:a, person:b], found follows:custom"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be written with INSERT: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "The record 'person:missing' does not exist"

[[test.results]]
value = "[]"

[[test.results]]
value = "[follows:[person:a, person:c]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE person;
DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
CREATE person:a, person:b, person:c RETURN NONE;

-- The edge id is canonical and derived; RELATE returns the synthesized record.
RELATE person:a->follows->person:b;
-- Repeating it is idempotent.
RELATE person:a->follows->person:b;
RELATE person:a->follows->person:c RETURN NONE;
RELATE person:b->follows->person:c RETURN NONE;
-- A self-loop is a single edge.
RELATE person:c->follows->person:c;

-- Reads synthesize the record everywhere.
SELECT VALUE ->follows FROM ONLY person:a;
SELECT VALUE ->follows->person FROM ONLY person:a;
SELECT VALUE <-follows FROM ONLY person:c;
SELECT VALUE <->follows FROM ONLY person:b;
SELECT VALUE ->follows.* FROM ONLY person:a;
SELECT * FROM follows;
SELECT count() FROM follows GROUP ALL;
SELECT count() FROM follows:[person:a, person:b] GROUP ALL;
SELECT count() FROM follows:[person:a, person:a]..=[person:b, person:zz] GROUP ALL;
SELECT VALUE id FROM follows ORDER BY id DESC LIMIT 2;
SELECT * FROM ONLY follows:[person:a, person:b];
RETURN follows:[person:a, person:b].out.id;
RETURN record::exists(follows:[person:a, person:c]);

-- Write shapes that assume stored records are rejected.
RELATE person:a->follows->person:b SET weight = 1;
RELATE person:a->follows:custom->person:b;
UPDATE follows:[person:a, person:b] SET weight = 1;
INSERT RELATION INTO follows { in: person:a, out: person:b };
-- ENFORCED is implied: a missing endpoint is rejected.
RELATE person:a->follows->person:missing;

-- Deletes: by id, by traversal, and by vertex cascade.
DELETE follows:[person:a, person:b];
SELECT VALUE ->follows FROM ONLY person:a;
DELETE person:c;
SELECT * FROM follows;
