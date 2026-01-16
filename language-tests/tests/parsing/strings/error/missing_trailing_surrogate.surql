/**
[test]

[test.results]
parsing-error = '''
String contains invalid escape sequence, unicode escape character is not a valid unicode character.
  --> [14:2]
   |
14 | "\uD83D";
   |  ^^^^^^ Not a valid character code
'''

*/
"\uD83D";
