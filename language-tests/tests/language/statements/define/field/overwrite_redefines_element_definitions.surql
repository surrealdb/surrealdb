/**
[env]
namespace = true
database = true
compatibility = 3

[test]
reason = """
DEFINE FIELD OVERWRITE defines the element definitions the new kind implies and removes the
ones the old kind implied and the new one does not, unless a field is defined in one: that one
stays, and is judged against the new kind like any field nested in it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ tags: 'DEFINE FIELD tags ON t TYPE array<string> PERMISSIONS FULL', "tags.*": 'DEFINE FIELD tags.* ON t TYPE string PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ id: t:1, tags: ['a'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ lines: 'DEFINE FIELD lines ON t TYPE array PERMISSIONS FULL', "lines.*": 'DEFINE FIELD lines.* ON t TYPE object PERMISSIONS FULL', "lines.*.name": 'DEFINE FIELD lines.*.name ON t TYPE string PERMISSIONS FULL', tags: 'DEFINE FIELD tags ON t TYPE array<string> PERMISSIONS FULL', "tags.*": 'DEFINE FIELD tags.* ON t TYPE string PERMISSIONS FULL' }"""

[[test.results]]
error = "Cannot give field `lines` the type `string`: field `lines.*` is defined in it, and only an object, array or set holds `*`"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD tags ON t TYPE array<array<string>>;
DEFINE FIELD OVERWRITE tags ON t TYPE array<string>;
(INFO FOR TABLE t).fields;
CREATE t:1 SET tags = ['a'];
DEFINE FIELD lines ON t TYPE array<object>;
DEFINE FIELD lines.*.name ON t TYPE string;
DEFINE FIELD OVERWRITE lines ON t TYPE array;
(INFO FOR TABLE t).fields;
DEFINE FIELD OVERWRITE lines ON t TYPE string;
