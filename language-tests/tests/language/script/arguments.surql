/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: article:test, summary: 'SurrealDB is awesome, advanced, cool' }]"

*/
LET $value = 'SurrealDB';
LET $words = ['awesome', 'advanced', 'cool'];
CREATE article:test SET summary = function($value, $words) {
	return `${arguments[0]} is ${arguments[1].join(', ')}`;
};
