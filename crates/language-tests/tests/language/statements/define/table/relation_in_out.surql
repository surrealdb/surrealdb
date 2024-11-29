/**
[test]
wip = true

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
error = "Found record: `likes:51zhfsvgh40e0u080b89` which is a relation, but expected a  RELATION IN record<person> OUT record<person | thing>"

[[test.results]]
error = "Found other:vdxv61a6ac8uci03jcr3 for field `out`, with record `likes:oh9r3pb9mhl3hkelejwi`, but expected a record<person | thing>"

[[test.results]]
error = "Found thing:d6dblop2vdsmlytue5dm for field `in`, with record `likes:j38df2332qc9s4nvnccf`, but expected a record<person>"

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
