/**
[test]

[[test.results]]
value = "[{ age: 18, id: person:test, name: 'John' }]"

[[test.results]]
value = "[{ age: 18, id: person:test, name: 'John' }]"

[[test.results]]
value = "[{ new_age: 30, old_age: 25 }]"

[[test.results]]
value = "[{ age: 35, name: 'John' }]"

[[test.results]]
value = "[{ age: 35, id: person:test, name: 'John' }]"

*/
CREATE person:test SET age = 18, name = 'John';
UPSERT person:test SET age = 25 RETURN VALUE $before;
UPSERT person:test SET age = 30 RETURN VALUE { old_age: $before.age, new_age: $after.age };
UPSERT person:test SET age = 35 RETURN age, name;
DELETE person:test RETURN VALUE $before;
