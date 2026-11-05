/**
[test]
reason = "A cast never takes component names."

[test.results]
parsing-error = """
Unexpected token `:`, expected a kind name
  --> [15:14]
   |
15 | RETURN <[city: string]> [1];
   |              ^
"""

*/
RETURN <[city: string]> [1];
