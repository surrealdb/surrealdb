/**
[test]
reason = "Test object destructuring in SELECT and OMIT with nested fields (new executor)"
*/

CREATE person:1 SET name = 'John', age = 21, obj = { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } };
SELECT obj.{ a, c.{ e, f } } FROM person;
SELECT * OMIT obj.c.{ d, f } FROM person;
