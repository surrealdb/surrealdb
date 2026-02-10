/**
[test]
reason = "Expected at least one THEN statement."

[test.results]
parsing-error = """
Expected at least one `THEN` statement
  --> [16:21]
   |
16 | DEFINE EVENT foo ON bar;
   |                     ^^^ `THEN` statement required
"""

*/

DEFINE EVENT foo ON bar;
