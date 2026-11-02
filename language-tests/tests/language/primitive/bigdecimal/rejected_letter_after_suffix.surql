/**
[test]
reason = "A bigdec suffix must end the number: a letter after it is refused"

[test.results]
parsing-error = """
Invalid token, found unexpected character `b` after number token
  --> [16:1]
   |
16 | 1bigdecimal;
   | ^^
"""

*/

1bigdecimal;
