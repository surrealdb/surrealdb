/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A test expectation can name a value of type `table`.
"""
issue = 7151

[[test.results]]
value = "hi"

[[test.results]]
value = "[hi, hi:1, 'hi']"
*/
RETURN type::table('hi');
RETURN [type::table('hi'), type::record('hi', 1), 'hi'];
