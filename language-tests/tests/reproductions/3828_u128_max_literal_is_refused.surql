/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
An integer literal past i128 is a parse error too.
"""
issue = 3828

[test.results]
parsing-error = """
Failed to parse number: number cannot fit within a 64bit signed integer
  --> [22:8]
   |
22 | RETURN 340282366920938463463374607431768211455;
   |        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""
*/
RETURN 340282366920938463463374607431768211455;
