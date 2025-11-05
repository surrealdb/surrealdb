/**
[test]

[test.results]
parsing-error = '''
Invalid escape sequence, expected `}` or hexadecimal character.
  --> [14:5]
   |
14 | "\u{G}";
   |     ^ Unexpected character
'''

*/
"\u{G}";
