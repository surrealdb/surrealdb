/**
[test]
reason = "Relation tables behind migration steps: the migrated fields of an edge, of its nested objects and of the records at its ends are what every traversal, edge scan, dereference and edge write reads, RELATE and INSERT RELATION after the step store migrated values, a write to an edge behind the step leaves its ends alone, and the ends of an edge are never migratable."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: works:a, in: person:1, meta: { rank: 7 }, out: org:1, since: 2019 }, { id: works:b, in: person:2, out: org:1, since: 0 }, { id: works:c, in: person:1, meta: { rank: -1 }, out: org:2, since: 2021 }]"

[[test.results]]
value = "[true, true, true]"

[[test.results]]
value = "[{ in: person:1, out: org:1, since: 2019 }, { in: person:1, out: org:2, since: 2021 }]"

[[test.results]]
value = "[works:b]"

[[test.results]]
value = "[2021, 2019, 0]"

[[test.results]]
value = "[{ c: 3, s: 4040 }]"

[[test.results]]
value = "[2019, 2021]"

[[test.results]]
value = "['Beta']"

[[test.results]]
value = "[[works:a, works:c], []]"

[[test.results]]
value = "[2019, 0]"

[[test.results]]
value = "['Ann', 'Bob']"

[[test.results]]
value = "[50, 0]"

[[test.results]]
value = "['Bob']"

[[test.results]]
value = "[50]"

[[test.results]]
value = "[50, 50, 0]"

[[test.results]]
value = "[{ out: { id: org:1, name: 'Acme', size: 50 } }]"

[[test.results]]
value = "[{ rank: 7 }, NONE, { rank: -1 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Couldn't coerce value for field `since` of `works:e`: Expected `int` but found `'2025'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2019, 0, 2021, 2024, 2026]"

[[test.results]]
value = "[0, 2024]"

[[test.results]]
value = "[{ id: works:b, in: person:2, out: org:1, since: 1 }]"

[[test.results]]
value = "[{ id: works:a, in: person:1, meta: { rank: 8 }, out: org:1, since: 2019 }]"

[[test.results]]
value = "[{ id: works:c, in: person:1, meta: { rank: -1 }, out: org:2, since: 4042 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[4042, 2026]"

[[test.results]]
value = "[2030, 1]"

[[test.results]]
error = "Cannot migrate field `in` of table `works`: `in` is fixed by the record's key"

[[test.results]]
error = "Cannot migrate field `out` of table `works`: `out` is fixed by the record's key"

[[test.results]]
value = "[{ field: 'since', generation: 5, name: 'since_int' }, { field: 'meta.rank', generation: 7, name: 'rank_int' }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'lvl3'"

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
value = "[2019, 0]"

[[test.results]]
value = "[org:1]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 2024]"

[[test.results]]
error = "Cannot migrate field `in` of table `works`: `in` is fixed by the record's key"
*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE org SCHEMAFULL;
DEFINE FIELD name ON org TYPE string;
DEFINE FIELD size ON org TYPE string;
DEFINE TABLE works TYPE RELATION IN person OUT org SCHEMAFULL;
DEFINE FIELD since ON works TYPE string;
DEFINE FIELD meta ON works TYPE option<object>;
DEFINE FIELD meta.rank ON works TYPE option<string>;
CREATE person:1 SET name = 'Ann' RETURN NONE;
CREATE person:2 SET name = 'Bob' RETURN NONE;
CREATE org:1 SET name = 'Acme', size = '50' RETURN NONE;
CREATE org:2 SET name = 'Beta', size = 'big' RETURN NONE;
RELATE person:1->works:a->org:1 SET since = '2019', meta = { rank: '7' } RETURN NONE;
RELATE person:2->works:b->org:1 SET since = 'never' RETURN NONE;
RELATE person:1->works:c->org:2 SET since = '2021', meta = { rank: 'x' } RETURN NONE;
ALTER FIELD since ON works TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
ALTER FIELD meta.rank ON works TYPE option<int> MIGRATE EXISTING ROWS AS rank_int USING <int>$value FALLBACK -1;
ALTER FIELD size ON org TYPE int MIGRATE EXISTING ROWS AS size_int USING <int>$value FALLBACK 0;
SELECT * FROM works ORDER BY id;
SELECT VALUE type::is_int(since) FROM works ORDER BY id;
SELECT in, out, since FROM works WHERE since > 2000 ORDER BY id;
SELECT VALUE id FROM works WHERE since = 0;
SELECT VALUE since FROM works ORDER BY since DESC;
SELECT count() AS c, math::sum(since) AS s FROM works GROUP ALL;
-- Traversals.
person:1->works.since;
person:1->works[WHERE since > 2020]->org.name;
SELECT VALUE ->works[WHERE since > 2000].id FROM person ORDER BY id;
SELECT VALUE <-works.since FROM ONLY org:1;
SELECT VALUE <-works<-person.name FROM ONLY org:1;
person:1->works->org.size;
person:1->works[WHERE since < 2020]->org<-works[WHERE since = 0]<-person.name;
SELECT VALUE ->works->org.size FROM ONLY person:2;
-- Dereference and fetch of the ends of an edge.
SELECT VALUE out.size FROM works ORDER BY id;
SELECT out FROM works:a FETCH out;
SELECT VALUE meta FROM works ORDER BY id;
-- Writes after the step.
RELATE person:2->works:d->org:2 SET since = 2024, meta = { rank: 3 } RETURN NONE;
RELATE person:2->works:e->org:2 SET since = '2025' RETURN NONE;
INSERT RELATION INTO works { id: works:f, in: person:1, out: org:2, since: 2026 } RETURN NONE;
SELECT VALUE since FROM works ORDER BY id;
person:2->works.since;
-- A write to an edge behind the step reads it migrated and leaves its ends alone.
UPDATE works:b SET since = since + 1 RETURN AFTER;
UPDATE works:a SET meta.rank = meta.rank + 1 RETURN AFTER;
UPDATE works:c SET since = since * 2 RETURN AFTER;
DELETE works:a;
RELATE person:2->works:a->org:1 SET since = 2030 RETURN NONE;
person:1->works.since;
SELECT VALUE <-works.since FROM ONLY org:1;
-- The ends of an edge are fixed by its key.
ALTER FIELD in ON works TYPE record<person> MIGRATE EXISTING ROWS AS m USING $value;
ALTER FIELD out ON works TYPE record<org> MIGRATE EXISTING ROWS AS m USING $value;
(INFO FOR TABLE works).migrations.migrations;
-- The same at compatibility level 3.
DEFINE DATABASE lvl3 COMPATIBILITY 3;
USE DB lvl3;
DEFINE TABLE works TYPE RELATION IN person OUT org SCHEMAFULL;
DEFINE FIELD since ON works TYPE string;
RELATE person:1->works:a->org:1 SET since = '2019' RETURN NONE;
RELATE person:2->works:b->org:1 SET since = 'never' RETURN NONE;
ALTER FIELD since ON works TYPE int MIGRATE EXISTING ROWS AS since_int USING <int>$value FALLBACK 0;
SELECT VALUE since FROM works ORDER BY id;
person:1->works[WHERE since > 2000]->org;
RELATE person:2->works:c->org:1 SET since = 2024 RETURN NONE;
person:2->works.since;
ALTER FIELD in ON works TYPE record<person> MIGRATE EXISTING ROWS AS m USING $value;
