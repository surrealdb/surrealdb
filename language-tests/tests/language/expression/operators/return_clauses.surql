/**
[test]
reason = "Test RETURN AFTER, BEFORE, DIFF, NONE and FIELDS on mutations"

[[test.results]]
value = "[{ age: 30, id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ age: 31, id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ age: 31, id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[[{ op: 'replace', path: '/age', value: 33 }]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 35, name: 'Alice' }]"

*/

CREATE person:1 SET name = 'Alice', age = 30;
UPDATE person:1 SET age = 31 RETURN AFTER;
UPDATE person:1 SET age = 32 RETURN BEFORE;
UPDATE person:1 SET age = 33 RETURN DIFF;
UPDATE person:1 SET age = 34 RETURN NONE;
UPDATE person:1 SET age = 35 RETURN name, age;
