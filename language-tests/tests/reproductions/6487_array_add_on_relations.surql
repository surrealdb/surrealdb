/**
[test]
reason = "Test array add on relations"
issue = 6487

[[test.results]]
value = "[{ id: person:one }, { id: person:two }]"

[[test.results]]
value = "[{ id: likes:1, in: person:one, out: person:one }]"

[[test.results]]
value = "[{ id: likes:2, in: person:one, out: person:two }]"

[[test.results]]
value = "[person:two, person:one, 'dogs']"

[[test.results]]
value = "[person:two, person:one]"
*/

CREATE person:one, person:two;
RELATE person:one->likes:1->person:one;
RELATE person:one->likes:2->person:two;

-- In issue fails with dogs added twice
person:one->likes->person.add('dogs');

-- In issue fails creating nested array
array::add(person:one->likes->person, [])->likes->person;
