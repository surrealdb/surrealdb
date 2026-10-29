/**
[test]
reason = "A graph lookup (->, <-, <->) ignores a FIELD clause on its subjects: FIELD names the referencing field of a reference lookup (<~) only, and a graph lookup answers the same edges with or without it."

[[test.results]]
value = "[{ id: person:1 }, { id: person:2 }, { id: post:1 }, { id: post:2 }]"

[[test.results]]
value = "[{ id: likes:1, in: person:1, out: post:1, weight: 1 }]"

[[test.results]]
value = "[{ id: likes:2, in: person:2, out: post:1, weight: 2 }]"

[[test.results]]
value = "[{ id: likes:3, in: person:1, out: post:2, weight: 3 }]"

[[test.results]]
value = "[{ id: likes:4, in: person:2, out: post:1, weight: 4 }]"

[[test.results]]
value = "[{ l: [likes:1, likes:3] }]"

[[test.results]]
value = "[{ l: [likes:1, likes:3] }]"

[[test.results]]
value = "[{ l: [likes:1, likes:2, likes:4] }]"

[[test.results]]
value = "[{ l: [likes:1, likes:2, likes:4] }]"

[[test.results]]
value = "[{ l: [likes:1, likes:3] }]"

[[test.results]]
value = "[{ l: [likes:3] }]"

[[test.results]]
value = "[likes:1, likes:3]"

[[test.results]]
value = "[likes:1, likes:2, likes:4]"

[[test.results]]
value = "[likes:3]"

[[test.results]]
value = "[{ weight: 4 }, { weight: 2 }, { weight: 1 }]"

[[test.results]]
value = "[likes:1]"

[[test.results]]
value = "[likes:1, likes:3, likes:3]"

[[test.results]]
value = "[likes:1, likes:3, likes:3]"

[[test.results]]
value = "[post:1, post:2]"

[[test.results]]
value = "[post:1, post:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ author: post:1, id: comment:1 }]"

[[test.results]]
value = "[{ author: post:2, id: comment:2, subject: post:1 }]"

[[test.results]]
value = "[comment:1, comment:2]"

[[test.results]]
value = "[comment:1]"

[[test.results]]
value = "[comment:2]"

*/

CREATE person:1, person:2, post:1, post:2;
RELATE person:1->likes:1->post:1 SET weight = 1;
RELATE person:2->likes:2->post:1 SET weight = 2;
RELATE person:1->likes:3->post:2 SET weight = 3;
RELATE person:2->likes:4->post:1 SET weight = 4;

-- The graph lookup in a projection, outbound and inbound, against the lookup without FIELD.
SELECT ->(likes FIELD weight) AS l FROM person:1;
SELECT ->likes AS l FROM person:1;
SELECT <-(likes FIELD weight) AS l FROM post:1;
SELECT <-likes AS l FROM post:1;
-- A field the edge table does not have.
SELECT ->(likes FIELD nosuchfield) AS l FROM person:1;
-- Both directions.
SELECT <->(likes FIELD weight) AS l FROM post:2;
-- The lookup on its own, on a record id.
person:1->(likes FIELD weight);
post:1<-(likes FIELD weight);
-- A lookup with clauses of its own.
person:1->(likes FIELD weight WHERE weight > 1);
post:1<-(SELECT weight FROM likes FIELD weight ORDER BY weight DESC);
person:1->(likes FIELD weight LIMIT 1);
-- Several subjects and a range subject.
person:1->(likes FIELD weight, likes:2..=4 FIELD in);
person:1->(likes, likes:2..=4);
-- A path through the edges to the vertices beyond them.
person:1->(likes FIELD weight)->post;
person:1->(likes FIELD weight)->(post FIELD x);
-- A reference lookup still reads FIELD.
DEFINE FIELD author ON comment TYPE record<post> REFERENCE;
DEFINE FIELD subject ON comment TYPE option<record<post>> REFERENCE;
CREATE comment:1 SET author = post:1;
CREATE comment:2 SET author = post:2, subject = post:1;
post:1<~comment;
post:1<~(comment FIELD author);
post:1<~(comment FIELD subject);
