/**
[test]
reason = """
A union whose array or set member has no element kind implies `any` for its elements: that member
describes them as anything, so the union of the element kinds admits anything, whatever its typed
members say. `array | array<int>` implies `v.* TYPE any`, as `option<array>` does. Written as the
union `any | int`, the definition could not be read back, since the kind grammar refuses `any` as a
member of a union, and every later statement that read the table's fields failed with a parse
error."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ v: 'DEFINE FIELD v ON t TYPE array | array<int> PERMISSIONS FULL', "v.*": 'DEFINE FIELD v.* ON t TYPE any PERMISSIONS FULL', w: 'DEFINE FIELD w ON t TYPE none | string PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ id: t:mixed, v: ['a', 1] }]"

[[test.results]]
value = "[{ id: t:ints, v: [1, 2] }]"

[[test.results]]
error = "Couldn't coerce value for field `v` of `t:text`: Expected `array | array<int>` but found `'text'`"

[[test.results]]
value = "[{ id: t:ints, v: [1, 2] }, { id: t:mixed, v: ['a', 1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ a: 'DEFINE FIELD a ON s TYPE array<int> | set PERMISSIONS FULL', "a.*": 'DEFINE FIELD a.* ON s TYPE any PERMISSIONS FULL', b: 'DEFINE FIELD b ON s TYPE array | set PERMISSIONS FULL', "b.*": 'DEFINE FIELD b.* ON s TYPE any PERMISSIONS FULL', n: 'DEFINE FIELD n ON s TYPE array<array | array<int>> PERMISSIONS FULL', "n.*": 'DEFINE FIELD n.* ON s TYPE array | array<int> PERMISSIONS FULL', "n.*.*": 'DEFINE FIELD n.*.* ON s TYPE any PERMISSIONS FULL' }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ o: 'DEFINE FIELD o ON f TYPE none | array PERMISSIONS FULL', "o.*": 'DEFINE FIELD o.* ON f TYPE any PERMISSIONS FULL', u: 'DEFINE FIELD u ON f TYPE none | array | array<int> PERMISSIONS FULL', "u.*": 'DEFINE FIELD u.* ON f TYPE any PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ id: f:1, o: [{ x: 1 }], u: [{ y: 2 }] }]"

*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD v ON t TYPE array | array<int>;
-- The table's definitions stay readable: another field is defined beside `v`,
DEFINE FIELD w ON t TYPE option<string>;
-- and INFO lists the definition `v` implies for its elements as `any`.
RETURN (INFO FOR TABLE t).fields;
-- The untyped member admits elements of any kind, and a value of no member is refused.
CREATE t:mixed SET v = ['a', 1];
CREATE t:ints SET v = [1, 2];
CREATE t:text SET v = 'text';
SELECT * FROM t ORDER BY id;

-- Either order of the members, a set, and a union of untyped members alike, and beneath an array
-- the union of untyped and typed element kinds implies `any` one level down.
DEFINE TABLE s SCHEMALESS;
DEFINE FIELD a ON s TYPE array<int> | set;
DEFINE FIELD b ON s TYPE array | set;
DEFINE FIELD n ON s TYPE array<array | array<int>>;
RETURN (INFO FOR TABLE s).fields;

-- A SCHEMAFULL table admits content nested in the elements, as it does beneath `option<array>`.
DEFINE TABLE f SCHEMAFULL;
DEFINE FIELD o ON f TYPE option<array>;
DEFINE FIELD u ON f TYPE option<array | array<int>>;
RETURN (INFO FOR TABLE f).fields;
CREATE f:1 SET o = [{ x: 1 }], u = [{ y: 2 }];
