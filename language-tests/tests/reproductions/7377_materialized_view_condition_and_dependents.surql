/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A non-aggregated view with a WHERE clause must keep its indexes, dependent views and events in step with the source table as rows enter and leave the view, and events must see the view record's own id."
issue = 7377

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
value = "[{ age: 30, id: parent:a, name: 'a' }]"

[[test.results]]
value = "[{ age: 10, id: parent:b, name: 'b' }]"

[[test.results]]
value = "[{ age: 30, name: 'a' }]"

[[test.results]]
value = "[{ age: 20, id: parent:b, name: 'b' }]"

[[test.results]]
value = "[{ age: 20, name: 'b' }, { age: 30, name: 'a' }]"

[[test.results]]
value = "[{ age: 20, name: 'b' }]"

[[test.results]]
value = "[{ age: 5, id: parent:a, name: 'a' }]"

[[test.results]]
value = "[{ age: 20, name: 'b' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ total: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ after: adults:a, before: NONE, kind: 'CREATE', who: 'a' }, { after: NONE, before: adults:a, kind: 'DELETE', who: 'a' }, { after: adults:b, before: NONE, kind: 'CREATE', who: 'b' }, { after: NONE, before: adults:b, kind: 'DELETE', who: 'b' }]"

*/

DEFINE TABLE parent SCHEMALESS;
DEFINE TABLE adults TYPE NORMAL SCHEMALESS AS SELECT * FROM parent WHERE age >= 18;
DEFINE INDEX adults_age ON adults FIELDS age;
DEFINE TABLE adult_count AS SELECT count() AS total FROM adults GROUP ALL;
DEFINE TABLE log SCHEMALESS;
DEFINE EVENT adults_changed ON adults WHEN true THEN (CREATE log SET kind = $event, who = $value.name, before = $before.id, after = $after.id);
CREATE parent:a SET name = 'a', age = 30;
CREATE parent:b SET name = 'b', age = 10;
SELECT name, age FROM adults ORDER BY age;
UPDATE parent:b SET age = 20;
SELECT name, age FROM adults ORDER BY age;
SELECT name, age FROM adults WHERE age = 20;
UPDATE parent:a SET age = 5;
SELECT name, age FROM adults ORDER BY age;
SELECT name, age FROM adults WHERE age = 30;
SELECT total FROM adult_count;
DELETE parent:b;
SELECT name, age FROM adults ORDER BY age;
SELECT total FROM adult_count;
SELECT kind, who, before, after FROM log ORDER BY who, kind;
