/**
[env]
namespace = true
database = true

[test]
reason = "SELECT ... PARALLEL, which reached the TiKV client from a non-runtime thread, is not part of the grammar, so the query is refused at parse time and never reaches storage."
issue = 5172

[test.results]
parsing-error = """
Unexpected token `PARALLEL`, expected Eof
  --> [20:17]
   |
20 | SELECT * FROM t PARALLEL;
   |                 ^^^^^^^^
"""
*/

SELECT * FROM t PARALLEL;
