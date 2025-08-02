/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, test: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, test: 1 }, { id: person:4, test: 4 }, { id: person:6, test: 6 }]"

*/

FOR $test IN [1, 2, 3] {
	IF $test == 2 {
		BREAK;
	};
	UPSERT type::thing('person', $test) SET test = $test;
};
SELECT * FROM person;

FOR $test IN [4, 5, 6] {
	IF $test == 5 {
		CONTINUE;
	};
	UPSERT type::thing('person', $test) SET test = $test;
};
SELECT * FROM person;

