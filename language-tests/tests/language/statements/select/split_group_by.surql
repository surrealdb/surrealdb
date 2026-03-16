/**
[test]

[test.results]
parsing-error = """
SPLIT and GROUP are mutually exclusive
  --> [19:20]
   |
19 | SELECT a, b FROM t SPLIT a, b GROUP BY a, b;
   |                    ^^^^^^^^^^ SPLIT cannot be used with GROUP
  --> [19:31]
   |
19 | SELECT a, b FROM t SPLIT a, b GROUP BY a, b;
   |                               ^^^^^^^^^^^^^ GROUP cannot be used with SPLIT
"""

*/

SELECT a, b FROM t SPLIT a, b GROUP BY a, b;
