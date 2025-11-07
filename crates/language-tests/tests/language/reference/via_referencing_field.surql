/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: city:london }, { id: city:newyork }]"

[[test.results]]
value = "[{ born_in: city:london, id: person:john, lives_in: city:newyork }]"

[[test.results]]
value = "[{ born_in: city:london, id: person:mary, lives_in: city:london }]"

[[test.results]]
value = "[person:john, person:mary, person:mary]"

[[test.results]]
value = "[person:john, person:mary]"

[[test.results]]
value = "[person:mary]"

[[test.results]]
value = "[person:john]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[person:john]"

*/

DEFINE FIELD born_in ON person TYPE record<city> REFERENCE;
DEFINE FIELD lives_in ON person TYPE record<city> REFERENCE;

CREATE city:london, city:newyork;
CREATE person:john SET born_in = city:london, lives_in = city:newyork;
CREATE person:mary SET born_in = city:london, lives_in = city:london;

city:london<~person;
city:london<~(person FIELD born_in);
city:london<~(person FIELD lives_in);

city:newyork<~person;
city:newyork<~(person FIELD born_in);
city:newyork<~(person FIELD lives_in);

