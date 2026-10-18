/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An integer literal past i64 is a parse error; nothing wraps negative.
"""
issue = 3828

[test.results]
parsing-error = """
Failed to parse number: number cannot fit within a 64bit signed integer
  --> [22:8]
   |
22 | RETURN 18446744073709551615;
   |        ^^^^^^^^^^^^^^^^^^^^
"""
*/
RETURN 18446744073709551615;
