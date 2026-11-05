/**
[env]
namespace = true
database = true

[test]
reason = """
	A field holding an array of records can carry the clause: every record in the
	array lists the referencing record. A write lists it with the records the array
	gained and takes it out of the lists of the records the array lost, and the
	other records the array still holds are untouched.
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
value = "[{ id: contact:a, opportunities: [] }, { id: contact:b, opportunities: [] }, { id: contact:c, opportunities: [] }]"

[[test.results]]
value = "[{ contacts: [contact:a, contact:b], id: opportunity:1 }]"

[[test.results]]
value = "[{ contacts: [contact:b], id: opportunity:2 }]"

[[test.results]]
value = "[[contact:a, [opportunity:1]], [contact:b, [opportunity:1, opportunity:2]], [contact:c, []]]"

[[test.results]]
value = "[{ contacts: [contact:b, contact:c], id: opportunity:1 }]"

[[test.results]]
value = "[[contact:a, []], [contact:b, [opportunity:1, opportunity:2]], [contact:c, [opportunity:1]]]"

[[test.results]]
value = "[{ contacts: [contact:b, contact:a], id: opportunity:2 }]"

[[test.results]]
value = "[[contact:a, [opportunity:2]], [contact:b, [opportunity:1, opportunity:2]], [contact:c, [opportunity:1]]]"

[[test.results]]
value = "[{ contacts: [], id: opportunity:1 }]"

[[test.results]]
value = "[[contact:a, [opportunity:2]], [contact:b, [opportunity:2]], [contact:c, []]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[[contact:a, []], [contact:b, []], [contact:c, []]]"

[[test.results]]
value = "[[contact:a, []], [contact:b, []], [contact:c, []]]"
*/

DEFINE TABLE contact SCHEMAFULL;
DEFINE FIELD opportunities ON contact TYPE array<record<opportunity>> DEFAULT [] READONLY;
DEFINE TABLE opportunity SCHEMAFULL;
DEFINE FIELD contacts ON opportunity TYPE array<record<contact>> REFERENCE MATERIALIZE INTO contact.opportunities;
CREATE contact:a, contact:b, contact:c;
CREATE opportunity:1 SET contacts = [contact:a, contact:b];
CREATE opportunity:2 SET contacts = [contact:b];
SELECT VALUE [id, opportunities] FROM contact ORDER BY id;
UPDATE opportunity:1 SET contacts = [contact:b, contact:c];
SELECT VALUE [id, opportunities] FROM contact ORDER BY id;
UPDATE opportunity:2 SET contacts += contact:a;
SELECT VALUE [id, opportunities] FROM contact ORDER BY id;
UPDATE opportunity:1 SET contacts = [];
SELECT VALUE [id, opportunities] FROM contact ORDER BY id;
DELETE opportunity:2;
SELECT VALUE [id, opportunities] FROM contact ORDER BY id;
SELECT VALUE [id, array::sort(<~opportunity)] FROM contact ORDER BY id;
