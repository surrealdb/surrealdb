/**
[test]
reason = """
	An `OMIT` path ending in a destructure removes the fields the destructure
	lists from the object the path reaches, and leaves that object and its
	other fields in place. A nested destructure entry removes the fields it
	lists one level down, and a destructure before a further step applies that
	step under each field it lists. The same holds when the projection names
	fields rather than `*`.
"""

[[test.results]]
value = "[{ id: person:1, list: [{ x: 1, y: 2 }, { x: 3, y: 4 }], obj: { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } } }]"

[[test.results]]
value = "[{ id: person:1, list: [{ x: 1, y: 2 }, { x: 3, y: 4 }], obj: { a: 1, b: 2, c: { e: 4 } } }]"

[[test.results]]
value = "[{ id: person:1, list: [{ x: 1, y: 2 }, { x: 3, y: 4 }], obj: { b: 2, c: { e: 4, f: 5 } } }]"

[[test.results]]
value = "[{ id: person:1, list: [{ x: 1, y: 2 }, { x: 3, y: 4 }], obj: { a: 1, b: 2, c: { e: 4, f: 5 } } }]"

[[test.results]]
value = "[{ id: person:1, list: [{ y: 2 }, { x: 3, y: 4 }], obj: { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } } }]"

[[test.results]]
value = "[{ obj: { a: 1, b: 2, c: { d: 3, f: 5 } } }]"

[[test.results]]
value = "[{ obj: { a: 1, c: { d: 3, e: 4, f: 5 } } }]"
*/

CREATE person:1 SET obj = { a: 1, b: 2, c: { d: 3, e: 4, f: 5 } }, list = [{ x: 1, y: 2 }, { x: 3, y: 4 }];

-- A trailing destructure removes the fields it lists, and nothing above them.
SELECT * OMIT obj.c.{ d, f } FROM person;

-- A nested entry removes the fields it lists from the field it names.
SELECT * OMIT obj.{ a, c.{ d } } FROM person;

-- A destructure before a further step reaches each field it lists.
SELECT * OMIT obj.{ c }.d FROM person;

-- An index reaches the element before the destructure applies.
SELECT * OMIT list[0].{ x } FROM person;

-- The same under a projection of named fields.
SELECT obj OMIT obj.c.{ e } FROM person;
SELECT obj OMIT obj.{ b } FROM person;
