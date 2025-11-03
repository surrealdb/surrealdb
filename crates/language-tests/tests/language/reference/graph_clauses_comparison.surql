/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:rand, likes: [cat:one, wolf:one], name: 'Rand' }]"

[[test.results]]
value = "[{ id: person:mat, likes: [cat:one], name: 'Mat' }]"

[[test.results]]
value = "[{ id: person:perrin, likes: [wolf:one], name: 'Perrin' }]"

[[test.results]]
value = "[{ id: likes:2r03gsbhr4tphepgucpn, in: person:rand, out: cat:one }, { id: likes:0uxr6lew4amszo1cwblb, in: person:rand, out: wolf:one }]"
skip-record-id-key=true

[[test.results]]
value = "[{ id: likes:uuyi9yei0lpj0h9lvjtc, in: person:mat, out: cat:one }]"
skip-record-id-key=true

[[test.results]]
value = "[{ id: likes:qu5frlor0z99jw8l4zxj, in: person:perrin, out: wolf:one }]"
skip-record-id-key=true

[[test.results]]
value = "[{ id: cat:one }, { id: wolf:one }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[env.capabilities]
allow-experimental = ["record_references"]

*/

DEFINE FIELD likes ON person TYPE array<record<cat|wolf>> REFERENCE;

CREATE person:rand SET name = "Rand", likes = [cat:one, wolf:one];
CREATE person:mat SET name = "Mat", likes = [cat:one];
CREATE person:perrin SET name = "Perrin", likes = [wolf:one];
RELATE person:rand->likes->[cat:one, wolf:one];
RELATE person:mat->likes->cat:one;
RELATE person:perrin->likes->wolf:one;
CREATE cat:one, wolf:one;

(SELECT VALUE <-likes<-person FROM ONLY cat:one) ALLINSIDE [person:rand, person:mat];
-- Note: same records, different order from above
(SELECT VALUE <~person FROM ONLY cat:one) ALLINSIDE [person:rand, person:mat];

(SELECT VALUE <-(SELECT * FROM likes WHERE in.name = "Rand" LIMIT 1)<-person FROM ONLY cat:one)
    = (SELECT VALUE <~(SELECT VALUE id FROM person WHERE name = "Rand" LIMIT 1) FROM ONLY cat:one);