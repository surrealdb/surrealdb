/**
[env]
namespace = true
database = true

[test]
reason = """
A field's kind defines the elements of the arrays and sets it admits, and a definition of them
written out as the kind implies it, which is how INFO lists it and an export writes it, is
admitted: a member of a union with no elements, such as `string`, says nothing of them, members
whose elements differ imply the union of their element kinds, and elements whose kind is a union
imply the elements of theirs. A field nested in an element is judged against the element's kind.
Any other kind is refused. The element definition judges the elements of each array or set the
field holds, and nothing of a value that has none. An import replays the definitions INFO lists.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ v: 'DEFINE FIELD v ON u TYPE set<float> | string PERMISSIONS FULL', "v.*": 'DEFINE FIELD v.* ON u TYPE float PERMISSIONS FULL' }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `v.*` with type `int` as it mismatched with field `v` with type `set<float> | string`"

[[test.results]]
value = """{ v: 'DEFINE FIELD v ON u TYPE set<float> | string PERMISSIONS FULL', "v.*": 'DEFINE FIELD v.* ON u TYPE float ASSERT $value >= 0 PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ id: u:set, v: {1.5f, 2f} }]"

[[test.results]]
value = "[{ id: u:text, v: 'text' }]"

[[test.results]]
error = "Found -1f for field `v.*`, with record `u:negative`, but field must conform to: $value >= 0"

[[test.results]]
error = "Couldn't coerce value for field `v` of `u:list`: Expected `set<float> | string` but found `['text']`"

[[test.results]]
value = "[{ id: u:set, v: {1.5f, 2f} }, { id: u:text, v: 'text' }]"

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
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD v.* ON l TYPE int | string PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `v.*` with type `int` as it mismatched with field `v` with type `array<int> | array<string>`"

[[test.results]]
value = "[{ id: l:ints, v: [1, 2] }]"

[[test.results]]
value = "[{ id: l:strings, v: ['a'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD v.*.* ON n TYPE bool PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `v.*.*` with type `int` as it mismatched with field `v` with type `array<float | array<bool>>`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: n:1, v: [1.5f, [true]], w: [{ name: 'a' }] }]"

[[test.results]]
value = "[{ id: n:2, v: [], w: 'text' }]"

[[test.results]]
error = "Couldn't coerce value for field `w.*.name` of `n:3`: Expected `string` but found `1`"

[[test.results]]
value = "[{ id: n:1, v: [1.5f, [true]], w: [{ name: 'a' }] }, { id: n:2, v: [], w: 'text' }]"

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
value = """{ v: 'DEFINE FIELD v ON restored TYPE set<float> | string PERMISSIONS FULL', "v.*": 'DEFINE FIELD v.* ON restored TYPE float ASSERT $value >= 0 PERMISSIONS FULL' }"""

[[test.results]]
error = "Found -1f for field `v.*`, with record `restored:negative`, but field must conform to: $value >= 0"

[[test.results]]
value = "[{ id: restored:text, v: 'text' }]"
*/

-- A member with no elements says nothing of `v.*`.
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD v ON u TYPE set<float> | string;
(INFO FOR TABLE u).fields;
DEFINE FIELD OVERWRITE v.* ON u TYPE float;
DEFINE FIELD OVERWRITE v.* ON u TYPE float ASSERT $value >= 0;
DEFINE FIELD OVERWRITE v.* ON u TYPE int;
(INFO FOR TABLE u).fields;

-- Each member is written, and the element definition judges the elements of the set alone.
CREATE u:set SET v = [2, 1.5];
CREATE u:text SET v = 'text';
CREATE u:negative SET v = [-1];
CREATE u:list SET v = ['text'];
SELECT * FROM u ORDER BY id;

-- An optional field, and one whose elements are optional, imply the same way.
DEFINE TABLE o SCHEMAFULL;
DEFINE FIELD v ON o TYPE option<array<int>> | string;
DEFINE FIELD OVERWRITE v.* ON o TYPE int;
DEFINE FIELD w ON o TYPE array<option<int>> | string;
DEFINE FIELD OVERWRITE w.* ON o TYPE option<int>;

-- Members whose elements differ imply the union of their element kinds.
DEFINE TABLE l SCHEMAFULL;
DEFINE FIELD v ON l TYPE array<int> | array<string>;
(INFO FOR TABLE l).fields['v.*'];
DEFINE FIELD OVERWRITE v.* ON l TYPE int | string;
DEFINE FIELD OVERWRITE v.* ON l TYPE int;
CREATE l:ints SET v = [1, 2];
CREATE l:strings SET v = ['a'];

-- Elements whose kind is a union imply the elements of their own arrays, and a field
-- nested in an element is judged against the element's kind.
DEFINE TABLE n SCHEMAFULL;
DEFINE FIELD v ON n TYPE array<float | array<bool>>;
(INFO FOR TABLE n).fields['v.*.*'];
DEFINE FIELD OVERWRITE v.*.* ON n TYPE bool;
DEFINE FIELD OVERWRITE v.*.* ON n TYPE int;
DEFINE FIELD w ON n TYPE array<object> | string;
DEFINE FIELD w.*.name ON n TYPE string;
CREATE n:1 SET v = [1.5, [true]], w = [{ name: 'a' }];
CREATE n:2 SET v = [], w = 'text';
CREATE n:3 SET v = [], w = [{ name: 1 }];
SELECT * FROM n ORDER BY id;

-- An import replays the definitions INFO lists: the element definition over the one the
-- field's kind implies.
OPTION IMPORT;
DEFINE TABLE OVERWRITE restored TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE v ON restored TYPE set<float> | string PERMISSIONS FULL;
DEFINE FIELD OVERWRITE v.* ON restored TYPE float ASSERT $value >= 0 PERMISSIONS FULL;
OPTION IMPORT = false;
(INFO FOR TABLE restored).fields;
CREATE restored:negative SET v = [-1];
CREATE restored:text SET v = 'text';
