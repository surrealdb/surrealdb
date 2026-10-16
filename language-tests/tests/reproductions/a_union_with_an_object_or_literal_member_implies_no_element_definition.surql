/**
[env]
namespace = true
database = true

[test]
reason = """
`*` reaches every element a value holds: an object's values and a literal array's elements as
surely as the elements of an `array<..>` or `set<..>`. A kind implies a definition for its
elements only when the elements of its array and set members are the only ones its values can
hold, so a union with an `object`, a literal array or object, or an `any` member implies none,
and each member's values are judged by the kind alone. In a SCHEMAFULL table an object member's
keys are fields of their own, as under `TYPE object`, unless the field is FLEXIBLE. A definition
written at `*` is judged against every member, and the element definitions a kind implies stop
at an element kind with such a member. INFO lists no element definition for such a kind, so an
import of what it lists rebuilds the table.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ v: 'DEFINE FIELD v ON b TYPE none | object | array<int> FLEXIBLE PERMISSIONS FULL', w: 'DEFINE FIELD w ON b TYPE none | object | array<int> PERMISSIONS FULL' }"

[[test.results]]
value = "[{ id: b:object, v: { x: 'text' } }]"

[[test.results]]
value = "[{ id: b:ints, v: [1, 2] }]"

[[test.results]]
error = "Couldn't coerce value for field `v` of `b:mixed`: Expected `none | object | array<int>` but found `[1, 'text']`"

[[test.results]]
error = "Found field 'w.x', but no such field exists for table 'b'"

[[test.results]]
value = "[{ id: b:ints, v: [1, 2] }, { id: b:object, v: { x: 'text' } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ v: 'DEFINE FIELD v ON lit TYPE none | [int, string] | array<bool> PERMISSIONS FULL', w: 'DEFINE FIELD w ON lit TYPE none | { a: string } | set<int> PERMISSIONS FULL' }"

[[test.results]]
value = "[{ id: lit:pair, v: [1, 'a'] }]"

[[test.results]]
value = "[{ id: lit:bools, v: [true, false] }]"

[[test.results]]
value = "[{ id: lit:object, w: { a: 'text' } }]"

[[test.results]]
value = "[{ id: lit:set, w: {1, 2} }]"

[[test.results]]
value = "[{ id: lit:bools, v: [true, false] }, { id: lit:object, w: { a: 'text' } }, { id: lit:pair, v: [1, 'a'] }, { id: lit:set, w: {1, 2} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ v: 'DEFINE FIELD v ON n TYPE array<object | array<int>> PERMISSIONS FULL', "v.*": 'DEFINE FIELD v.* ON n TYPE object | array<int> PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ id: n:1, v: [{ x: 'text' }, [1]] }]"

[[test.results]]
value = "[{ id: n:1, v: [{ x: 'text' }, [1]] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `v.*` with type `string` as it mismatched with field `v` with type `object | array<int>`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `v.*` of `e:object`: Expected `int` but found `'text'`"

[[test.results]]
value = "[{ id: e:ints, v: [1] }]"

[[test.results]]
error = "Cannot set field `v.*` with type `bool` as it mismatched with field `v` with type `none | [int, string] | array<bool>`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ v: 'DEFINE FIELD v ON restored TYPE [int, string] | array<bool> PERMISSIONS FULL' }"

[[test.results]]
value = "[{ id: restored:pair, v: [1, 'a'] }]"
*/

-- An object member's values are elements no array member describes.
DEFINE TABLE b SCHEMAFULL;
DEFINE FIELD v ON b TYPE option<object | array<int>> FLEXIBLE;
DEFINE FIELD w ON b TYPE option<object | array<int>>;
(INFO FOR TABLE b).fields;
CREATE b:object SET v = { x: 'text' };
CREATE b:ints SET v = [1, 2];
CREATE b:mixed SET v = [1, 'text'];
CREATE b:closed SET w = { x: 'text' };
SELECT * FROM b ORDER BY id;

-- A literal array or object member's elements are not those of the array or set member.
DEFINE TABLE lit SCHEMAFULL;
DEFINE FIELD v ON lit TYPE option<[int, string] | array<bool>>;
DEFINE FIELD w ON lit TYPE option<{ a: string } | set<int>>;
(INFO FOR TABLE lit).fields;
CREATE lit:pair SET v = [1, 'a'];
CREATE lit:bools SET v = [true, false];
CREATE lit:object SET w = { a: 'text' };
CREATE lit:set SET w = [2, 1];
SELECT * FROM lit ORDER BY id;

-- The definitions a kind implies stop at an element kind with an object member.
DEFINE TABLE n SCHEMALESS;
DEFINE FIELD v ON n TYPE array<object | array<int>>;
(INFO FOR TABLE n).fields;
CREATE n:1 SET v = [{ x: 'text' }, [1]];
SELECT * FROM n ORDER BY id;

-- A definition written at `*` is judged against every member, and judges every element.
DEFINE TABLE e SCHEMALESS;
DEFINE FIELD v ON e TYPE object | array<int>;
DEFINE FIELD v.* ON e TYPE string;
DEFINE FIELD v.* ON e TYPE int;
CREATE e:object SET v = { x: 'text' };
CREATE e:ints SET v = [1];
DEFINE FIELD OVERWRITE v.* ON lit TYPE bool;

-- An import of what INFO lists rebuilds the table.
OPTION IMPORT;
DEFINE TABLE OVERWRITE restored TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE v ON restored TYPE [int, string] | array<bool> PERMISSIONS FULL;
OPTION IMPORT = false;
(INFO FOR TABLE restored).fields;
CREATE restored:pair SET v = [1, 'a'];
