/**
[test]

[[test.results]]
value = "[{ id: person:1, one: 'one', three: 'three', two: 'two' }]"

[[test.results]]
value = "[{ count: 1 }]"

*/
-- This will return the created record
UPSERT person:1 SET one = 'one', two = 'two', three = 'three';
-- Select all created records
SELECT count() FROM person GROUP ALL;
