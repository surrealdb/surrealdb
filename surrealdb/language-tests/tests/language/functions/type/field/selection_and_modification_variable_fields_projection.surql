/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ embedded: { name: 'post' }, id: person:test, name: 'one', now: d'2023-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ name: 'two' }]"

[[test.results]]
value = "[{ name: 'two' }]"

[[test.results]]
value = "[{ date: d'2024-01-01T00:00:00Z', edit: 2, id: log:test }]"

*/

DEFINE EVENT test ON person WHEN true THEN {
	UPSERT log:test SET edit += 1, date = $after.now WHERE type::field('name') != 'test';
};
LET $field = 'name';
UPSERT person:test SET now = d'2023-01-01', name = 'one', embedded[$field] = 'post';
UPSERT person:test SET now = d'2024-01-01', name = 'two' RETURN type::field($field);
SELECT type::field($field) FROM person;
SELECT * FROM log;
