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
match = '''string::matches($error,"Found thing:[a-zA-Z0-9]* for field `out`, with record `likes:[a-zA-Z0-9]*`, but expected a record<person>")'''
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:4iz6dy5ylsy3si35iym1, in: person:gnirh7xd5itm6s2axxf5, out: thing:mvv6klyc507qq0nf5c08 }]"
skip-record-id-key = true

[[test.results]]
match = '''string::matches($error,"Found other:[a-zA-Z0-9]* for field `out`, with record `likes:[a-zA-Z0-9]*`, but expected a record<person \\| thing>")'''
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: likes:sdydy481hv0x3y502glm, in: person:gnirh7xd5itm6s2axxf5, out: other:ew3kht2mdxvrnz0pzw8z }]"
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
