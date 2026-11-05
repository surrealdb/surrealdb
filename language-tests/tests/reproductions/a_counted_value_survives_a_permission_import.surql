/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:bob, name: 'bob' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: hidden:1 }, { id: hidden:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: part:1 }]"

[[test.results]]
value = "[{ a: 2, id: part:2 }]"

[[test.results]]
value = "[{ a: 1, id: part:3 }]"
*/

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
CREATE person:bob SET name = 'bob';
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN (SELECT * FROM person WHERE name = $name)
	DURATION FOR SESSION 1h;

DEFINE TABLE hidden SCHEMALESS PERMISSIONS FOR select NONE;
CREATE hidden:1, hidden:2;

DEFINE TABLE part SCHEMALESS PERMISSIONS FOR select WHERE a = 1;
CREATE part:1 SET a = 1;
CREATE part:2 SET a = 2;
CREATE part:3 SET a = 1;
