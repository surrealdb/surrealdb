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
match = "$error = /Couldn't coerce value for field `out` of `likes:[0-9a-z]+`: Expected `record<person>` but found `thing:[0-9a-z]+`/"
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:loxurikpwrb2o242gsgu, in: person:euupzy7py15hmh1rbant, out: thing:adr45xnyi8cdakbctq7p }]"
skip-record-id-key = true

[[test.results]]
match = "$error = <regex> 'Couldn\\'t coerce value for field `out` of `likes:[0-9a-z]+`: Expected `record<person|thing>` but found `other:[0-9a-z]+`'"
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:ae4n1ktf3mv23xfyzmn9, in: person:euupzy7py15hmh1rbant, out: other:jzrul8flc9jqe175lg76 }]"
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
