/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Integer addition past i64::MAX is an error, not a wrapped negative.
"""
issue = 3828

[[test.results]]
error = "Cannot perform addition with '9223372036854775807' and '1'"
*/
RETURN 9223372036854775807 + 1;
