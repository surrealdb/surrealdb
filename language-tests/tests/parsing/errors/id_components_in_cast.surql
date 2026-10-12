/**
[test]
reason = "A cast never takes component names."

[test.results]
parsing-error = """
Unexpected token `an identifier`, expected a kind name
  --> [15:10]
   |
15 | RETURN <[city: string]> [1];
   |          ^^^^
"""

*/
RETURN <[city: string]> [1];
