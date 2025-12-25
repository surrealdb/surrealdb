/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: a:2 }, { id: a:3 }]"

[[test.results]]
value = "[{ references: [a:2, a:3], id: a:1 }]"

[[test.results]]
value = "[{ id: edge:1, in: a:1, out: a:2 }]"

[[test.results]]
value = "[{ id: edge:2, in: a:1, out: a:3 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/


DEFINE FIELD references ON a TYPE option<array<record<a>>> REFERENCE;

CREATE a:2, a:3;
CREATE a:1 SET references = [a:2, a:3];

RELATE a:1->edge:1->a:2;
RELATE a:1->edge:2->a:3;

a:2<~a = a:2<-edge<-a;
a:3<~a = a:3<-edge<-a;