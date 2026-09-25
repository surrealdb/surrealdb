/**
[test]
reason = """
Grammar boundary for the EXPLAIN matrix in this directory. LIVE SELECT registers
a subscription rather than running a plan, and rejects the trailing EXPLAIN its
non-live counterpart accepts.
"""

[test.results]
parsing-error = """
Unexpected token `EXPLAIN`, expected Eof
  --> [19:22]
   |
19 | LIVE SELECT * FROM t EXPLAIN;
   |                      ^^^^^^^
"""
*/

LIVE SELECT * FROM t EXPLAIN;
