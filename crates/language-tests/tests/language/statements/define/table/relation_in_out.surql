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
value = "NONE"

[[test.results]]
value = "[{ id: likes:7qt6148b3e2jargx90lw, in: person:a6cld8fhzwbrwhlfnbju, out: thing:d6dblop2vdsmlytue5dm }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: likes:qvwx0qdpo7b0mkjnqeq1, in: person:a6cld8fhzwbrwhlfnbju, out: person:vxh1w4c8355aj88owv73 }]"
skip-record-id-key = true

[[test.results]]
match = "$error = Found record: `likes:[a-zA-Z0-9]*` which is a relation, but expected a  RELATION IN record<person> OUT record<person | thing>"
error = true

[[test.results]]
match = "$error = Found other:[a-zA-Z0-9]* for field `out`, with record `likes:[a-zA-Z0-9]*`, but expected a record<person | thing>"
error = true

[[test.results]]
match = "$error = Found thing:[a-zA-Z0-9]* for field `in`, with record `likes:[a-zA-Z0-9]*`, but expected a record<person>"
error = true

*/

DEFINE TABLE likes TYPE RELATION FROM person TO person | thing SCHEMAFUL;
LET $first_p = CREATE person SET name = 'first person';
LET $second_p = CREATE person SET name = 'second person';
LET $thing = CREATE thing SET name = 'rust';
LET $other = CREATE other;
RELATE $first_p->likes->$thing;
RELATE $first_p->likes->$second_p;
CREATE likes;
RELATE $first_p->likes->$other;
RELATE $thing->likes->$first_p;
