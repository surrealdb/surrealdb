/**
[test]
reason = "Test graph direction operators: ->, <-, <-> to exercise Dir enum"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:alice }]"

[[test.results]]
value = "[{ id: person:bob }]"

[[test.results]]
value = "[{ id: person:charlie }]"

[[test.results]]
value = "[{ id: knows:3v6yynxk8mbljzsn1zzv, in: person:alice, out: person:bob }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: knows:9wowjtlyizlhnfualyzf, in: person:bob, out: person:charlie }]"
skip-record-id-key = true

[[test.results]]
value = '[{ "->knows": { "->person": [person:bob] } }]'

[[test.results]]
value = '[{ "<-knows": { "<-person": [person:alice] } }]'

[[test.results]]
value = '[{ "<->knows": { "<->person": [person:alice, person:bob, person:bob, person:charlie] } }]'

*/

DEFINE TABLE person;
DEFINE TABLE knows TYPE RELATION IN person OUT person;
CREATE person:alice;
CREATE person:bob;
CREATE person:charlie;
RELATE person:alice->knows->person:bob;
RELATE person:bob->knows->person:charlie;

-- Outgoing
SELECT ->knows->person FROM person:alice;
-- Incoming
SELECT <-knows<-person FROM person:bob;
-- Both directions
SELECT <->knows<->person FROM person:bob;
