/**
[test]
reason = "Test that MAXDEPTH throws an error in absence of ASYNC."

[test.results]
parsing-error = """
Unexpected token `MAXDEPTH`
  --> [16:27]
   |
16 | DEFINE EVENT test ON user MAXDEPTH 2 WHEN true THEN (
   |                           ^^^^^^^^ MAXDEPTH must be set after ASYNC
"""

*/

DEFINE EVENT test ON user MAXDEPTH 2 WHEN true THEN (
    SLEEP 0s
);
