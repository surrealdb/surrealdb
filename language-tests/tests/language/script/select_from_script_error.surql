/**
[test]

[[test.results]]
error = """
Problem with embedded script function. An exception occurred: error
"""

[[test.results]]
error = """
Problem with embedded script function. An exception occurred: error
"""

*/
SELECT * FROM function() {
	throw 'error';
};
SELECT * FROM function() {
	throw new Error('error');
};
