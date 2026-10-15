/**
[test]
reason = "A REFERENCE field is always top-level, and its backlinks are written from every value beneath it, so a migration or a removal that rewrites or clears one of them from a definition inside it leaves a backlink pointing at a link no read shows: a migration of the element definition of a REFERENCE field, and a removal of it, are refused on an enforced table with rows until the REFERENCE field is removed. The definition of the REFERENCE field itself is judged where it is made."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Cannot migrate field `refs.*` of table `tb`: it lies within the REFERENCE field `refs`, whose backlinks hold its stored values"

[[test.results]]
error = "Cannot change field `refs.*` of table `tb`: it lies within the REFERENCE field `refs`, whose backlinks hold its stored values: remove that field first"

[[test.results]]
value = "[{ id: tb:1, refs: [person:1] }]"

[[test.results]]
value = "NONE"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD refs ON tb TYPE option<array<record<person>>> REFERENCE;
CREATE tb:1 SET refs = [person:1] RETURN NONE;
-- The element definition lies within the REFERENCE field.
ALTER FIELD refs.* ON tb TYPE record<person> MIGRATE EXISTING ROWS AS m USING $value FALLBACK person:0;
REMOVE FIELD refs.* ON tb;
SELECT * FROM tb;
-- The REFERENCE field itself is judged where it is made.
REMOVE FIELD refs ON tb;
