/**
[test]
reason = """
Grammar boundary for the EXPLAIN matrix in this directory. The index allow-list
clause belongs to the target of a mutating statement, not to its tail: UPDATE
accepts `UPDATE t WITH NOINDEX SET ...` (exercised in mutating_statements.surql)
and fails to parse when the clause follows the SET list.
"""

[test.results]
parsing-error = """
Unexpected token `WITH`, expected Eof
  --> [20:20]
   |
20 | UPDATE t SET n = 1 WITH INDEX idx WHERE n = 1;
   |                    ^^^^
"""
*/

UPDATE t SET n = 1 WITH INDEX idx WHERE n = 1;
