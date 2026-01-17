/**
[test]

[test.results]
parsing-error = """
Expected a single result output when using the ONLY keyword
  --> [15:21]
   |
15 | SELECT * FROM ONLY 1, 2, 3, person, 4, [1, 2, 3];
   |                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^ ONLY keyword can only be used with a single table expression
"""

*/

SELECT * FROM ONLY 1, 2, 3, person, 4, [1, 2, 3];
