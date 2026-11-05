/**
[test]
reason = """
Grammar boundary for the EXPLAIN matrix in this directory. INSERT takes neither
form of EXPLAIN: the trailing keyword is not part of its grammar, so the
statement fails to parse rather than producing a plan.
"""

[test.results]
parsing-error = """
Unexpected token `EXPLAIN`, expected Eof
  --> [19:38]
   |
19 | INSERT INTO t { id: t:1, name: 'a' } EXPLAIN;
   |                                      ^^^^^^^
"""
*/

INSERT INTO t { id: t:1, name: 'a' } EXPLAIN;
