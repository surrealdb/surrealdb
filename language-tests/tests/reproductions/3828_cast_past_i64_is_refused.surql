/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
Casting a decimal past i64 to int is refused, not saturated or wrapped.
"""
issue = 3828

[[test.results]]
error = "Could not cast into `int` using input `18446744073709551615dec`"
*/
RETURN <int>18446744073709551615dec;
