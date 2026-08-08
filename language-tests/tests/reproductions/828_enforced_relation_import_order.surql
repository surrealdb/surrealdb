/**
[env]
namespace = true
database = true

[test]
reason = "An ENFORCED relation replayed under OPTION IMPORT accepts edges whose endpoints have not been restored yet, and enforces them again once the import ends"
issue = 828

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[person:b]]"

[[test.results]]
value = "[[person:a]]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The record 'person:missing' does not exist"

*/

OPTION IMPORT;
DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION FROM person TO person ENFORCED;
-- An export orders tables by name, so the edge table's `INSERT RELATION`
-- arrives before either endpoint exists. This is the statement the exporter
-- emits, verbatim.
INSERT RELATION [{ id: knows:1, in: person:a, out: person:b }];
INSERT [{ id: person:a }, { id: person:b }];
SELECT VALUE ->knows->person FROM person:a;
SELECT VALUE <-knows<-person FROM person:b;
-- Leaving import mode restores enforcement.
OPTION IMPORT = false;
RELATE person:a->knows->person:missing;
