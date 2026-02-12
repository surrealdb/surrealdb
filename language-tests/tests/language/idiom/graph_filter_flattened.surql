/**
[test]
reason = "Test that graph traversal with filters produces correctly flattened results"

[[test.results]]
value = "[{ id: person:1 }, { id: person:2 }, { id: person:3 }]"

[[test.results]]
value = "[{ id: likes:1, in: person:1, out: person:2 }]"

[[test.results]]
value = "[{ id: likes:2, in: person:2, out: person:3 }]"

[[test.results]]
value = "[person:3]"

[[test.results]]
value = "[person:3]"

[[test.results]]
value = "[person:2]"

[[test.results]]
value = "[[person:2]]"

[[test.results]]
value = "[[person:3]]"

[[test.results]]
value = "[{ likes: [person:2] }, { likes: [person:3] }, { likes: [] }]"
*/

CREATE person:1, person:2, person:3;
RELATE person:1->likes:1->person:2;
RELATE person:2->likes:2->person:3;
person:1->likes->person->likes->person;
person:1->likes->person[?true]->likes->person;
person:1->likes->person[?true];
[person:1][?true]->likes->person;
[person:1]->likes->person[?true]->likes->person;
SELECT ->likes[?true]->person as likes FROM person;
