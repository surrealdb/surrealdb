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
value = "[{ id: likes:06jb1s1givlhh18tygxo, in: person:89o00cip2xw07f2w2upq, out: thing:m05avew5bzxn1fprfv48 }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: likes:i1mv1jai8ir01d3ih7qd, in: person:89o00cip2xw07f2w2upq, out: person:oieqdx61ygylnxi00qaw }]"
skip-record-id-key = true


[[test.results]]
match = "$error = <regex> 'Found record: `likes:[a-z0-9]+` which is a relation, but expected a  RELATION IN record<person> OUT record<person | thing>'"
error = true

[[test.results]]
match = "$error = <regex> 'Couldn\\'t coerce value for field `out` of `likes:[a-z0-9]+`: Expected `record<person|thing>` but found `other:[0-9a-z]+`'"
error = true

[[test.results]]
match = "$error = /Couldn't coerce value for field `in` of `likes:[a-z0-9]+`: Expected `record<person>` but found `thing:[0-9a-z]+`/"
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
