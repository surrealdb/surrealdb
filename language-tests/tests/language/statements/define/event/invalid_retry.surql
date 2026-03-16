/**
[test]
reason = "Test that RETRY throws an error in absence of ASYNC."

[test.results]
parsing-error = """
Unexpected token `RETRY`
  --> [16:27]
   |
16 | DEFINE EVENT test ON user RETRY 5 WHEN true THEN (
   |                           ^^^^^ RETRY must be set after ASYNC
"""

*/

DEFINE EVENT test ON user RETRY 5 WHEN true THEN (
    SLEEP 0s
);
