/**
[env]
namespace = true
database = true

[test]
reason = """
REMOVE FIELD removes the element definitions a field's kind implied whatever the field is
named: removing a field named by a keyword, such as `select`, removes `select.*` with it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ select: 'DEFINE FIELD `select` ON t TYPE array<string> PERMISSIONS FULL', "select.*": 'DEFINE FIELD `select`.* ON t TYPE string PERMISSIONS FULL' }"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "{  }"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD `select` ON t TYPE array<string>;
(INFO FOR TABLE t).fields;
REMOVE FIELD `select` ON t;
(INFO FOR TABLE t).fields;
