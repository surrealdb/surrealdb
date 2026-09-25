/**
[test]
reason = """
Grammar boundary for the EXPLAIN matrix in this directory. CREATE takes neither
form of EXPLAIN: the trailing keyword is not part of its grammar, so the
statement fails to parse rather than producing a plan. A parse error aborts the
whole file, which is why each rejected statement needs a file of its own.
"""

[test.results]
parsing-error = """
Unexpected token `EXPLAIN`, expected Eof
  --> [20:27]
   |
20 | CREATE t:1 SET name = 'a' EXPLAIN;
   |                           ^^^^^^^
"""
*/

CREATE t:1 SET name = 'a' EXPLAIN;
