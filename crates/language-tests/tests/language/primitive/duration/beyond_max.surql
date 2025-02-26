/**
[test]

[test.results]
parsing-error = """
Invalid token, failed to parse duration digits: number too large to fit in target type
  --> [15:1]
   |
15 | 18446744073709551616s
   | ^^^^^^^^^^^^^^^^^^^^^ 
"""

*/
/// 1 nano second higher then maximum duration
18446744073709551616s
