/**
[env]
backend = ["mem", "surrealkv"]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A delete whose REFERENCE ON DELETE CASCADE or UNSET action runs an event
	of the referencing table that holds a schema statement changes the
	catalog: a write after it in the block applies the definitions the event
	made, as main applies them.
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: parent:1 }, { id: parent:2 }]"

[[test.results]]
value = "[{ id: child:1, p: parent:1 }]"

[[test.results]]
value = "[{ id: kin:1, p: parent:2 }]"

[[test.results]]
value = "[{ a: 'cascade', id: note:1 }]"

[[test.results]]
value = "[{ a: 'cascade', b: 'unset', id: note:2 }]"

[[test.results]]
value = "[{ a: 'cascade', id: note:1 }, { a: 'cascade', b: 'unset', id: note:2 }]"
*/
DEFINE TABLE parent SCHEMALESS;
DEFINE TABLE child SCHEMALESS;
DEFINE FIELD p ON child TYPE option<record<parent>> REFERENCE ON DELETE CASCADE;
DEFINE TABLE kin SCHEMALESS;
DEFINE FIELD p ON kin TYPE option<record<parent>> REFERENCE ON DELETE UNSET;
DEFINE TABLE note SCHEMALESS;
DEFINE EVENT gone ON child WHEN $event = 'DELETE' THEN { DEFINE FIELD OVERWRITE a ON note VALUE 'cascade' };
DEFINE EVENT unset ON kin WHEN $event = 'UPDATE' THEN { DEFINE FIELD OVERWRITE b ON note VALUE 'unset' };
CREATE parent:1, parent:2;
CREATE child:1 SET p = parent:1;
CREATE kin:1 SET p = parent:2;
{ DELETE parent:1; CREATE note:1 RETURN AFTER };
{ DELETE parent:2; CREATE note:2 RETURN AFTER };
SELECT * FROM note;
