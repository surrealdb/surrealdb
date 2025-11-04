/**
[test]

[[test.results]]
value = "[{ id: person:test, name: 'Jaime' }]"

[[test.results]]
value = "[{ id: person:test, name: 'Tobie' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

*/
-- This will return the created record as no matching records exist
UPSERT person:test SET name = 'Jaime' WHERE name = 'Jaime';
-- This will return the updated record, because a matching record exists, and the WHERE clause matches
UPSERT person:test SET name = 'Tobie' WHERE name = 'Jaime';
-- This will return a newly created record, because the WHERE clause does not match
UPSERT person:test SET name = 'Tobie' WHERE name = 'Jaime';
-- Select all created records
SELECT count() FROM person GROUP ALL;
