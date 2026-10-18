/**
[env]
timeout = 5000

[test]
reason = "Pins current behaviour, which the issue reports as a defect: adding REFERENCE to a field that already holds records writes no reference keys for them, so <~ finds none until a record is rewritten with the link changed (an UPDATE that sets the same value does not write them). Not changed here."
issue = 7144

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
value = "[{ id: child:c1, name: 'child1' }]"

[[test.results]]
value = "[{ child_ref: child:c1, id: parent:p1, name: 'parent1' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: child:c1, refs: [] }]"

[[test.results]]
value = "[{ child_ref: child:c1, id: parent:p1, name: 'parent1' }]"

[[test.results]]
value = "[{ id: child:c1, refs: [] }]"

*/

DEFINE TABLE child SCHEMAFULL;
DEFINE FIELD name ON child TYPE string;
DEFINE TABLE parent SCHEMAFULL;
DEFINE FIELD name ON parent TYPE string;
DEFINE FIELD child_ref ON parent TYPE record<child>;
CREATE child:c1 SET name = 'child1';
CREATE parent:p1 SET name = 'parent1', child_ref = child:c1;
DEFINE FIELD OVERWRITE child_ref ON parent TYPE record<child> REFERENCE ON DELETE REJECT;
SELECT id, <~parent AS refs FROM child:c1;
UPDATE parent:p1 SET child_ref = child:c1;
SELECT id, <~parent AS refs FROM child:c1;
