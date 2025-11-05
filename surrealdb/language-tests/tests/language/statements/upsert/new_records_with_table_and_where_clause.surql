/**
[test]

[[test.results]]
value = "[{ id: person:lhlyishztxmg3zarjr2d, name: 'Jaime' }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: person:lhlyishztxmg3zarjr2d, name: 'Tobie' }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: person:lhlyishztxmg3zarjr2d, name: 'Tobie' }]"
skip-record-id-key = true

[[test.results]]
value = "[{ count: 2 }]"

*/

-- This will return the created record as no matching records exist
UPSERT person SET name = 'Jaime' WHERE name = 'Jaime';
-- This will return the updated record, because a matching record exists, and the WHERE clause matches
UPSERT person SET name = 'Tobie' WHERE name = 'Jaime';
-- This will return a newly created record, because the WHERE clause does not match
UPSERT person SET name = 'Tobie' WHERE name = 'Jaime';
-- Select all created records
SELECT count() FROM person GROUP ALL;
