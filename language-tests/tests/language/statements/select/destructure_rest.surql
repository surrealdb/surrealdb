/**
[test]
reason = "The `...` entry of an object destructure keeps every field the pattern's other entries did not already claim, and does not shadow an entry that renamed a field it also covers"

[[test.results]]
value = "[{ age: 21, id: person:1, name: 'John', obj: { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } } }]"

[[test.results]]
value = "{ a: 1, b: 2, c: { d: 3, e: 4, f: 5 } }"

[[test.results]]
value = "{ z: 1, a: 1, b: 2, c: { d: 3, e: 4, f: 5 } }"

[[test.results]]
value = "{ a: 1, b: 2, c: { d: 3, e: 4, f: 5 } }"
*/

CREATE person:1 SET name = 'John', age = 21, obj = { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } };
SELECT VALUE obj.{ a, ... } FROM ONLY person:1;
SELECT VALUE obj.{ z: a, ... } FROM ONLY person:1;
SELECT VALUE obj.{ ... } FROM ONLY person:1;
