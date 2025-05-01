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
match = """$error = /Couldn't coerce value for field `out` of `likes:[0-9a-z]+`: Expected `record<person>` but found `thing:[0-9a-z]+`/"""
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:60l77luoejr1s5th7d4r, in: person:15nrffr4aoneyjt6l2ft, out: thing:z956x3ako31hccv18y1s }]"
skip-record-id-key = true

[[test.results]]
match = """$error = <regex> 'Couldn\\'t coerce value for field `out` of `likes:[0-9a-z]+`: Expected `record<person|thing>` but found `other:[0-9a-z]+`'"""
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:hov8mp192d8d2mpdlprv, in: person:15nrffr4aoneyjt6l2ft, out: other:vf1kjuaou5kpx1zq1yq5 }]"
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
