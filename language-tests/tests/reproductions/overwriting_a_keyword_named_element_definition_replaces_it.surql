/**
[env]
namespace = true
database = true

[test]
reason = """
An element definition a kind implies is stored under the same key as an explicit definition of
its path, whatever the field is named. For a field named by a keyword, `DEFINE FIELD OVERWRITE
`select`.*` replaces the `select.*` that `TYPE array<string>` implied instead of writing a second
definition beside it. The table lists one definition of the path: `INFO FOR TABLE ... STRUCTURE`
lists every stored definition, where the plain form is keyed by path and shows one either way.
Redefining `select` as `array<int>` rewrites that one definition's kind, so `[1]` is written, and
REMOVE FIELD leaves no definition of the path behind.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ select: 'DEFINE FIELD `select` ON t TYPE array<string> PERMISSIONS FULL', "select.*": "DEFINE FIELD `select`.* ON t TYPE string ASSERT $value != '' PERMISSIONS FULL" }"""

[[test.results]]
value = "['`select`', '`select`.*']"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ select: 'DEFINE FIELD `select` ON t TYPE array<int> PERMISSIONS FULL', "select.*": "DEFINE FIELD `select`.* ON t TYPE int ASSERT $value != '' PERMISSIONS FULL" }"""

[[test.results]]
value = "[{ id: t:1, select: [1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ select: 'DEFINE FIELD `select` ON t TYPE array<int> PERMISSIONS FULL' }"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD `select` ON t TYPE array<string>;
DEFINE FIELD OVERWRITE `select`.* ON t TYPE string ASSERT $value != '';
(INFO FOR TABLE t).fields;
(INFO FOR TABLE t STRUCTURE).fields.name;
DEFINE FIELD OVERWRITE `select` ON t TYPE array<int>;
(INFO FOR TABLE t).fields;
CREATE t:1 SET `select` = [1];
REMOVE FIELD `select`.* ON t;
(INFO FOR TABLE t).fields;
