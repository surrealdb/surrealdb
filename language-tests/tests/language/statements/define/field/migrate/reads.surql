/**
[test]
reason = "Every read path applies a committed step to rows stored before it: scans, filters, ordering, grouping, point gets, record links, graph hops and field permissions."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: '41', id: person:1, name: 'Ann' }]"

[[test.results]]
value = "[{ age: '37', friend: person:1, id: person:2, name: 'Bob' }]"

[[test.results]]
value = "[{ age: 'old', id: person:3, name: 'Cy' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[41, 37, 0]"

[[test.results]]
value = "['Ann']"

[[test.results]]
value = "['Cy', 'Bob', 'Ann']"

[[test.results]]
value = "[{ c: 3, t: true }]"

[[test.results]]
value = "41"

[[test.results]]
value = "41"

[[test.results]]
value = "41"

[[test.results]]
value = "[41]"

[[test.results]]
value = "[{ s: 78 }]"
*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE string;
DEFINE FIELD friend ON person TYPE option<record<person>>;
CREATE person:1 SET name = 'Ann', age = '41';
CREATE person:2 SET name = 'Bob', age = '37', friend = person:1;
CREATE person:3 SET name = 'Cy', age = 'old';
DEFINE TABLE knows TYPE RELATION;
RELATE person:2->knows->person:1 RETURN NONE;
ALTER FIELD age ON person TYPE int MIGRATE EXISTING ROWS AS age_to_int USING <int>$value FALLBACK 0;
SELECT VALUE age FROM person ORDER BY id;
SELECT VALUE name FROM person WHERE age > 38;
SELECT VALUE name FROM person ORDER BY age;
SELECT type::is_int(age) AS t, count() AS c FROM person GROUP BY t;
person:1.age;
SELECT VALUE age FROM ONLY person:1;
SELECT VALUE friend.age FROM ONLY person:2;
SELECT VALUE ->knows->person.age FROM ONLY person:2;
SELECT math::sum(age) AS s FROM person GROUP ALL;
