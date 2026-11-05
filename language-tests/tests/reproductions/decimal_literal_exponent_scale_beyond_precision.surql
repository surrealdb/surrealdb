/**
[test]
reason = "An exponent-form decimal literal whose scale exceeds the decimal precision is a parse error naming the limit, never a silently rounded value"

[test.results]
parsing-error = """
Failed to parse decimal: Scale exceeds the maximum precision allowed: 29 > 28
  --> [15:8]
   |
15 | RETURN 1.5e-28dec;
   |        ^^^^^^^^^^
"""
*/

RETURN 1.5e-28dec;
