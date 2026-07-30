/**
[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "REMOVE MODULE requires `::` to follow the preceding path segment with no intervening whitespace, so `mod :: test` is rejected with the caret on the segment before the separator."

[test.results]
parsing-error = """
Unexpected whitespace
  --> [18:15]
   |
18 | REMOVE MODULE mod :: test;
   |               ^^^ No whitespace allowed after this token
"""
*/

REMOVE MODULE mod :: test;
