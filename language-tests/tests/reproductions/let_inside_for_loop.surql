/**
[test]
reason = "LET inside FOR loop should not panic with 'Set statement outside of block'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:test, name: 'Stella Berry' }]"

*/

FOR $name IN ["Stella Berry"] {
	LET $created = CREATE ONLY person:test SET name = $name;
};

SELECT * FROM person;
