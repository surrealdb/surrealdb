/**
[test]
reason = "A graph lookup that names a FIELD answers its edges as one without it, wherever it stands. Found by the 3.x/4.x differential fuzzer. language/graph/field_clause.surql covers the clause forms."

[[test.results]]
value = "[{ id: person:1 }, { id: post:1 }]"

[[test.results]]
value = "[{ id: likes:1, in: person:1, out: post:1, weight: 1 }]"

[[test.results]]
value = "[{ id: likes:3, in: person:1, out: post:1, weight: 3 }]"

[[test.results]]
value = "[{ l: [likes:1, likes:3] }]"

[[test.results]]
value = "[{ l: [likes:1, likes:3] }]"

[[test.results]]
value = "[{ l: [likes:1, likes:3] }]"

[[test.results]]
value = "[likes:1, likes:3]"

*/

CREATE person:1, post:1;
RELATE person:1->likes:1->post:1 SET weight = 1;
RELATE person:1->likes:3->post:1 SET weight = 3;
SELECT ->(likes FIELD weight) AS l FROM person:1;
SELECT <-(likes FIELD weight) AS l FROM post:1;
SELECT ->(likes FIELD nosuchfield) AS l FROM person:1;
person:1->(likes FIELD weight);
