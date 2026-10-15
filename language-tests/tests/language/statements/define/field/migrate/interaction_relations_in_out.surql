/**
[test]
wip = true
reason = "Decision open: narrowing the TYPE of `in` or `out` on an enforced relation table with edges is accepted; it cannot take MIGRATE. An enforced relation table with edges refuses a change of the type of `in` or `out` that its existing edges might not satisfy, since the ends of an edge are fixed by its key and cannot be migrated; widening is accepted, and a table that is not enforced accepts what it accepts today."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "string::contains($error, '`in`') && string::contains($error, 'works')"
error = true

[[test.results]]
match = "string::contains($error, '`out`') && string::contains($error, 'works')"
error = true

[[test.results]]
value = "'DEFINE FIELD in ON works TYPE record<person | org> PERMISSIONS FULL'"

[[test.results]]
value = "'DEFINE FIELD out ON works TYPE record<org | person> PERMISSIONS FULL'"

[[test.results]]
value = "[{ id: works:a, in: person:1, out: org:1, since: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'lvl3', namespace: 'test' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD in ON works TYPE record<org> PERMISSIONS FULL'"
*/
DEFINE TABLE works TYPE RELATION IN person OUT org SCHEMAFULL;
DEFINE FIELD since ON works TYPE int;
RELATE person:1->works:a->org:1 SET since = 1 RETURN NONE;
RELATE person:2->works:b->org:1 SET since = 2 RETURN NONE;
-- Widening keeps every edge valid.
DEFINE FIELD OVERWRITE in ON works TYPE record<person | org>;
DEFINE FIELD OVERWRITE out ON works TYPE record<org | person>;
-- Narrowing to a type the edges of person do not satisfy.
DEFINE FIELD OVERWRITE in ON works TYPE record<org>;
ALTER FIELD out ON works TYPE record<person>;
(INFO FOR TABLE works).fields.in;
(INFO FOR TABLE works).fields.out;
-- Every edge is still writable.
UPDATE works:a SET since = 5 RETURN AFTER;
RELATE person:3->works:c->org:1 SET since = 3 RETURN NONE;
-- Narrowing to a type every edge satisfies is accepted.
DEFINE FIELD OVERWRITE in ON works TYPE record<person>;
-- A table that is not enforced accepts the narrowing, as it always has.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE works TYPE RELATION IN person OUT org SCHEMAFULL;
RELATE person:1->works:a->org:1 RETURN NONE;
DEFINE FIELD OVERWRITE in ON works TYPE record<org>;
(INFO FOR TABLE works).fields.in;
