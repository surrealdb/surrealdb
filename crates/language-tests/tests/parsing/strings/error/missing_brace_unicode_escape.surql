/**
[test]

[test.results]
parsing-error = '''
Invalid escape sequence, expected `}` or hexadecimal character.
  --> [14:9]
   |
14 | "\u{00B0";
   |         ^ Unexpected character
'''

*/
"\u{00B0";
