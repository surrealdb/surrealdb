/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, test: 1 }]"

*/

FOR $test IN 1..4 {
	IF $test == 2 {
		BREAK;
	};
	UPSERT type::thing('person', $test) SET test = $test;
};
SELECT * FROM person;
