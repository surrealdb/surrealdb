/**
[test]
reason = "Test ORDER BY field that is NOT in SELECT - this produces a parsing error"

[test.results]
parsing-error = """
Missing order idiom `age` in statement selection
  --> [22:33]
   |
22 | SELECT id, name FROM t ORDER BY age ASC;
   |                                 ^^^^^^^
  --> [22:8]
   |
22 | SELECT id, name FROM t ORDER BY age ASC;
   |        ^^^^^^^^ Idiom missing here
"""

*/

-- Test: ORDER BY field not in SELECT produces a parsing error
-- This is expected behavior in SurrealDB - ORDER BY fields must be in the SELECT list
SELECT id, name FROM t ORDER BY age ASC;
