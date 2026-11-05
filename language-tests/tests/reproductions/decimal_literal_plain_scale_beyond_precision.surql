/**
[test]
reason = "A plain-notation decimal literal with significant digits beyond the decimal precision is a parse error naming the limit, never a silently rounded value"

[test.results]
parsing-error = """
Failed to parse decimal: Scale exceeds the maximum precision allowed: 31 > 28
  --> [15:8]
   |
15 | RETURN 0.0000000000000000000000000000001dec;
   |        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""
*/

RETURN 0.0000000000000000000000000000001dec;
