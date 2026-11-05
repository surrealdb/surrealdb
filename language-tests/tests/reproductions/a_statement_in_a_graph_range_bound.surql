/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A statement written as a key part of a graph step's record range is evaluated
as that bound's value. A FOR there is a statement in value position like an
IF, a SELECT or a block, and its value is NONE: `likes:[(FOR ...)]..` reads
every edge keyed by an array, and `likes:..[(FOR ...)]` every edge keyed below
one. The same holds for each direction, for a bounded range, under a WHERE,
before a further step and in a projection. A bound is evaluated where the
statement stands, before the step reads the value it starts from, so a FOR
over NONE raises the error a FOR raises over a value it cannot iterate even
where the step has no record to start from.
"""

[[test.results]]
value = "[{ id: person:1 }, { id: person:2 }, { id: person:3 }]"

[[test.results]]
value = "[{ id: likes:[1], in: person:1, out: person:2 }]"

[[test.results]]
value = "[{ id: likes:[5], in: person:1, out: person:3 }]"

[[test.results]]
value = "[{ id: likes:2, in: person:1, out: person:3 }]"

[[test.results]]
value = "[{ id: likes:3, in: person:2, out: person:1 }]"

[[test.results]]
error = "Cannot execute statement using value: NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[likes:2]"

[[test.results]]
value = "[likes:[1], likes:[5]]"

[[test.results]]
value = "[likes:3]"

[[test.results]]
value = "[likes:[1]]"

[[test.results]]
value = "[likes:[1]]"

[[test.results]]
value = "[likes:[5]]"

[[test.results]]
value = "[person:2, person:3]"

[[test.results]]
value = "[{ l: [likes:[1], likes:[5]] }]"

[[test.results]]
value = "[likes:[1], likes:[5]]"

[[test.results]]
value = "[likes:[5]]"

[[test.results]]
value = "[likes:[5]]"

[[test.results]]
value = "[likes:[5]]"

*/

CREATE person:1, person:2, person:3;
RELATE person:1->likes:[1]->person:2;
RELATE person:1->likes:[5]->person:3;
RELATE person:1->likes:2->person:3;
RELATE person:2->likes:3->person:1;

->likes:..[(FOR $l IN NONE {;})];
->likes:..[(FOR $l IN [] {;})];
person:1->likes:..[(FOR $l IN [] {;})];
person:1->likes:[(FOR $l IN [] {;})]..;
person:1<-likes:..[(FOR $l IN [] {;})];
person:2<->likes:[(FOR $l IN [] {;})]..;
person:1->likes:[(FOR $l IN [] {;})]..[5];
person:1->(likes:[(FOR $l IN [] {;})].. WHERE out = person:3);
person:1->likes:[(FOR $l IN [] {;})]..->person;
SELECT ->likes:[(FOR $l IN [] {;})].. AS l FROM person:1;
person:1->likes:[(FOR $l IN [1, 2] { CREATE tick; })]..;
person:1->likes:[(IF true { 5 })]..;
person:1->likes:[{ 5 }]..;
person:1->likes:[(SELECT VALUE 5 FROM ONLY 1)]..;
