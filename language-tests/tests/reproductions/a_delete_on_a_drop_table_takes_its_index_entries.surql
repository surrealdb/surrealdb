/**
[test]
reason = "A DELETE on a DROP table removes the stored record, so it removes the record's index entries too, whether the statement names the record or a vertex's cascade reaches the edge: once DROP is lifted, a UNIQUE index admits the deleted value again and a COUNT index counts only the rows storage holds."

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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[dd:3]"

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
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 2 }]"

*/

DEFINE TABLE dd SCHEMALESS;
DEFINE INDEX dd_u ON dd FIELDS a UNIQUE;
DEFINE INDEX dd_c ON dd COUNT;
CREATE dd:1 SET a = 1 RETURN NONE;
CREATE dd:2 SET a = 2 RETURN NONE;
DEFINE TABLE OVERWRITE dd DROP;
DELETE dd:2 RETURN NONE;
DEFINE TABLE OVERWRITE dd SCHEMALESS;
-- The COUNT index answers this one, and a scan the other: they agree.
SELECT count() FROM dd GROUP ALL;
array::len(SELECT * FROM dd);
SELECT VALUE id FROM dd WHERE a = 2;
-- The UNIQUE index holds no entry for the deleted record's value.
CREATE dd:3 SET a = 2 RETURN NONE;
SELECT count() FROM dd GROUP ALL;
SELECT VALUE id FROM dd WHERE a = 2;

-- An edge of a DROP relation table, deleted by its vertex's cascade.
DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION IN person OUT person SCHEMALESS;
DEFINE INDEX knows_u ON knows FIELDS tag UNIQUE;
DEFINE INDEX knows_c ON knows COUNT;
CREATE person:1, person:2, person:3 RETURN NONE;
RELATE person:1->knows:a->person:2 SET tag = 'x' RETURN NONE;
RELATE person:2->knows:b->person:3 SET tag = 'y' RETURN NONE;
DEFINE TABLE OVERWRITE knows TYPE RELATION IN person OUT person DROP SCHEMALESS;
DELETE person:1 RETURN NONE;
DEFINE TABLE OVERWRITE knows TYPE RELATION IN person OUT person SCHEMALESS;
SELECT count() FROM knows GROUP ALL;
array::len(SELECT * FROM knows);
RELATE person:2->knows:c->person:3 SET tag = 'x' RETURN NONE;
SELECT count() FROM knows GROUP ALL;
