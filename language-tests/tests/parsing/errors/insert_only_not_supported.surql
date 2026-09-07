/**
[test]
reason = "INSERT has no ONLY keyword, unlike the five other mutating statements. `ONLY` is taken as the target table name, leaving the `INTO` that follows with nowhere to go."

[test.results]
parsing-error = """
Unexpected token `INTO`, expected Eof
  --> [14:13]
   |
14 | INSERT ONLY INTO t { id: 1 };
   |             ^^^^
"""
*/
INSERT ONLY INTO t { id: 1 };
