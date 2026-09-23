/**
[test]
reason = """
Grammar boundary for the EXPLAIN matrix in this directory. The prefix EXPLAIN
applies to SELECT but not to LIVE SELECT, and the parser reads the leading
EXPLAIN as the start of an expression, so the failure lands on LIVE's own SELECT
keyword rather than on EXPLAIN.
"""

[test.results]
parsing-error = """
Unexpected token `SELECT`, expected the query to end
  --> [20:14]
   |
20 | EXPLAIN LIVE SELECT * FROM t;
   |              ^^^^^^ maybe forgot a semicolon after the previous statement?
"""
*/

EXPLAIN LIVE SELECT * FROM t;
