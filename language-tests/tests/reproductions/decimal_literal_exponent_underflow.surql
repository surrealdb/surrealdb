/**
[test]
reason = "An exponent-form decimal literal far below the smallest representable scale is a parse error rather than zero"

[test.results]
parsing-error = """
Failed to parse decimal: Scale exceeds the maximum precision allowed: 400 > 28
  --> [15:8]
   |
15 | RETURN 1e-400dec;
   |        ^^^^^^^^^
"""
*/

RETURN 1e-400dec;
