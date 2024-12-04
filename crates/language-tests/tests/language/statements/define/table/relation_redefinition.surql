/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$error = /Found thing:[a-zA-Z0-9]* for field `out`, with record `likes:[a-zA-Z0-9]*`, but expected a record<person>/"
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:dxkha0qcosudw16pgszg, in: person:qn7qdiuw7er59ygnow8m, out: thing:c9ptwxoc3pppmwzkuk8y }]"
skip-record-id-key = true

[[test.results]]
match = "$error = /Found other:[a-zA-Z0-9]* for field `out`, with record `likes:[a-zA-Z0-9]*`, but expected a record<person | thing>/"
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:wax0na23c4ueso3ymp9d, in: person:qn7qdiuw7er59ygnow8m, out: other:s5dt338gm6thhtpy8h26 }]"
skip-record-id-key = true

*/

DEFINE TABLE likes TYPE RELATION IN person OUT person;
LET $person = CREATE person;
LET $thing = CREATE thing;
LET $other = CREATE other;
RELATE $person->likes->$thing;
REMOVE TABLE likes;
DEFINE TABLE likes TYPE RELATION IN person OUT person | thing;
RELATE $person->likes->$thing;
RELATE $person->likes->$other;
REMOVE FIELD out ON TABLE likes;
DEFINE FIELD out ON TABLE likes TYPE record<person | thing | other>;
RELATE $person->likes->$other;
