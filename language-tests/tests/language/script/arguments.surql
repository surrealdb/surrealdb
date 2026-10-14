/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: article:test, summary: 'SurrealDB is awesome, advanced, cool' }]"

*/
LET $name = 'SurrealDB';
LET $words = ['awesome', 'advanced', 'cool'];
CREATE article:test SET summary = function($name, $words) {
	return `${arguments[0]} is ${arguments[1].join(', ')}`;
};
