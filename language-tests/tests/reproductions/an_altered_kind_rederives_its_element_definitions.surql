/**
[env]
namespace = true
database = true
compatibility = 3

[test]
reason = """
ALTER FIELD … TYPE defines the element definitions the new kind implies, as DEFINE FIELD
OVERWRITE does: once `tags` is `array<int>`, `tags.*` is `int` and an array of ints is
written. A definition the old kind implied and the new one does not goes with the old kind.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ tags: 'DEFINE FIELD tags ON t TYPE array<int> PERMISSIONS FULL', "tags.*": 'DEFINE FIELD tags.* ON t TYPE int PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ id: t:1, tags: [1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ grid: 'DEFINE FIELD grid ON t TYPE array<int> PERMISSIONS FULL', "grid.*": 'DEFINE FIELD grid.* ON t TYPE int PERMISSIONS FULL', tags: 'DEFINE FIELD tags ON t TYPE array<int> PERMISSIONS FULL', "tags.*": 'DEFINE FIELD tags.* ON t TYPE int PERMISSIONS FULL' }"""

[[test.results]]
value = "[{ grid: [1, 2], id: t:2, tags: [] }]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD tags ON t TYPE array<string>;
ALTER FIELD tags ON t TYPE array<int>;
(INFO FOR TABLE t).fields;
CREATE t:1 SET tags = [1];
DEFINE FIELD grid ON t TYPE array<array<int>>;
ALTER FIELD grid ON t TYPE array<int>;
(INFO FOR TABLE t).fields;
CREATE t:2 SET tags = [], grid = [1, 2];
