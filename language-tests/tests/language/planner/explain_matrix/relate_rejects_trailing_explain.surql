/**
[test]
reason = """
Grammar boundary for the EXPLAIN matrix in this directory. RELATE takes neither
form of EXPLAIN: the trailing keyword is not part of its grammar, so the
statement fails to parse rather than producing a plan.
"""

[test.results]
parsing-error = """
Unexpected token `EXPLAIN`, expected Eof
  --> [19:22]
   |
19 | RELATE t:1->rel->t:2 EXPLAIN;
   |                      ^^^^^^^
"""
*/

RELATE t:1->rel->t:2 EXPLAIN;
