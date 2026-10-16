/**
[test]
run = false
*/

-- Fixture for the `mutate_*` write-path tests, imported after `_schema.surql`: the
-- `person` rows it seeds gain an ASSERT on `age`, a unique index on `name`, and one event
-- on update and one on create that each create a row of `audit`.
DEFINE FIELD age ON person TYPE option<int> ASSERT $value = NONE OR $value > 0;
DEFINE INDEX person_name ON person FIELDS name UNIQUE;
DEFINE EVENT person_updated ON person WHEN $event = 'UPDATE'
	THEN (CREATE audit SET what = 'update', who = $after.name);
DEFINE EVENT person_created ON person WHEN $event = 'CREATE'
	THEN (CREATE audit SET what = 'create', who = $after.name);
