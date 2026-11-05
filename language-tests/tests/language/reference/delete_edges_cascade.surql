/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A delete takes the edges joined to the record with it, and the references those edges hold"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }]"

[[test.results]]
value = "[{ id: knows:ab, in: person:a, out: person:b }]"

[[test.results]]
value = "[{ id: knows:bc, in: person:b, out: person:c }]"

[[test.results]]
value = "[{ id: knows:ca, in: person:c, out: person:a }]"

[[test.results]]
value = "[{ about: knows:ab, id: note:1 }]"

[[test.results]]
value = "[{ about: knows:bc, id: note:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: knows:bc, in: person:b, out: person:c }]"

[[test.results]]
value = "[{ about: knows:bc, id: note:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:b }]"
*/

DEFINE TABLE person;
DEFINE TABLE knows TYPE RELATION IN person OUT person;
DEFINE TABLE note;
DEFINE FIELD about ON note TYPE option<record<knows>> REFERENCE ON DELETE CASCADE;
CREATE person:a, person:b, person:c;
RELATE person:a->knows:ab->person:b;
RELATE person:b->knows:bc->person:c;
RELATE person:c->knows:ca->person:a;
CREATE note:1 SET about = knows:ab;
CREATE note:2 SET about = knows:bc;
DELETE person:a;
SELECT id, in, out FROM knows ORDER BY id;
SELECT * FROM note ORDER BY id;
DELETE person:c;
SELECT id, in, out FROM knows ORDER BY id;
SELECT * FROM note ORDER BY id;
SELECT * FROM person ORDER BY id;
