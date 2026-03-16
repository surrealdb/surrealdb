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
value = "[{ age: 25, can_drive: true, friend: person:b, id: person:a }]"

[[test.results]]
value = "[{ age: 30, can_drive: true, friend: person:a, id: person:b }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNUP ( CREATE type::record('person', $id) SET age = $age )
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE OVERWRITE person SCHEMAFULL
	PERMISSIONS
		FOR select WHERE friend.can_drive = true
		FOR create, update, delete FULL;

DEFINE FIELD age ON person TYPE int;
DEFINE FIELD can_drive ON person VALUE age >= 18;
DEFINE FIELD friend ON person TYPE option<record<person>>;

CREATE person:a SET age = 25, friend = person:b;
CREATE person:b SET age = 30, friend = person:a;
