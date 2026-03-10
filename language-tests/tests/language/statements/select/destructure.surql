/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test object destructuring in SELECT and OMIT with nested fields"

[[test.results]]
value = "[{ age: 21, id: person:1, name: 'John', obj: { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } } }]"

[[test.results]]
value = "[{ obj: { a: 1, c: { e: 4, f: 5 } } }]"

[[test.results]]
value = "[{ age: 21, id: person:1, name: 'John', obj: { a: 1, b: 2, c: { e: 4 } } }]"
*/

CREATE person:1 SET name = 'John', age = 21, obj = { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } };
SELECT obj.{ a, c.{ e, f } } FROM person;
SELECT * OMIT obj.c.{ d, f } FROM person;
