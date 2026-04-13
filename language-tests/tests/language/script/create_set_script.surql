/**
[test]

[[test.results]]
value = '''[{ bio: 'Line 1\nLine 2', id: person:test, scores: [66f, 84f, 73f] }]'''

*/
CREATE person:test SET scores = function() {
	return [6.6, 8.4, 7.3].map(v => v * 10);
}, bio = function() {
	return "Line 1\nLine 2";
};
