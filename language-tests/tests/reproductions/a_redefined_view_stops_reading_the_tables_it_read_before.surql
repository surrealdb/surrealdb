/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A redefinition withdraws the registrations the table's previous view definition
held on the tables it read, whatever the new definition is. A view redefined
over another table, or as a plain table, is no longer written by the table it
read before, `INFO FOR TABLE` of that table no longer lists it, and that table
can be removed.
"""

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
value = "{  }"

[[test.results]]
value = "{ v: 'DEFINE TABLE v TYPE ANY SCHEMALESS AS SELECT a FROM other PERMISSIONS NONE' }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 2, id: v:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE src SCHEMALESS;
DEFINE TABLE other SCHEMALESS;
DEFINE TABLE v AS SELECT a FROM src;
DEFINE TABLE w AS SELECT a FROM src;
DEFINE TABLE OVERWRITE v AS SELECT a FROM other;
DEFINE TABLE OVERWRITE w SCHEMALESS;
(INFO FOR TABLE src).tables;
(INFO FOR TABLE other).tables;
CREATE src:1 SET a = 1 RETURN NONE;
CREATE other:1 SET a = 2 RETURN NONE;
SELECT * FROM v ORDER BY id;
SELECT * FROM w ORDER BY id;
REMOVE TABLE src;
