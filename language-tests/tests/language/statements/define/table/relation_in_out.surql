/**
[test]
reason = "Test DEFINE TABLE TYPE RELATION with IN/OUT constraints"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:alice }]"

[[test.results]]
value = "[{ id: post:first, title: 'Hello' }]"

[[test.results]]
value = "[{ id: likes:h4hx3b2umz37rx0g1tht, in: person:alice, out: post:first }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: likes:h4hx3b2umz37rx0g1tht, in: person:alice, out: post:first }]"
skip-record-id-key = true

[[test.results]]
value = "{ events: {  }, fields: { in: 'DEFINE FIELD in ON likes TYPE record<person> PERMISSIONS FULL', out: 'DEFINE FIELD out ON likes TYPE record<post> PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

*/

DEFINE TABLE likes TYPE RELATION IN person OUT post;
DEFINE TABLE person;
DEFINE TABLE post;
CREATE person:alice;
CREATE post:first SET title = 'Hello';
RELATE person:alice->likes->post:first;
SELECT * FROM likes;
INFO FOR TABLE likes;
