/**
[test]
reason = "A tuple that starts without a name is an ordinary tuple, so a later name is an unexpected token."

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected a kind name
  --> [15:42]
   |
15 | DEFINE FIELD id ON reading TYPE [string, time: datetime];
   |                                          ^^^^
"""

*/
DEFINE FIELD id ON reading TYPE [string, time: datetime];
