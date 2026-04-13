CREATE person:test SET scores = function() {
	return [6.6, 8.4, 7.3].map(v => v * 10);
}, bio = function() {
	return "Line 1\nLine 2";
};
