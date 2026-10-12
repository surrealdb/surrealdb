/**
[env]
namespace = true
database = true

[test]
reason = """
REMOVE FIELD removes the element definitions a field's kind implied whatever the field is
named. An element definition is stored under its escaped name, which differs from the raw one
for a field named by a keyword, so removing `select` removes `select.*` under either key.
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
