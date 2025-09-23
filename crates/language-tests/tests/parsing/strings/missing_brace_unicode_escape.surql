/**
[test]

[test.results]
parsing-error = '''
Invalid escape sequence, expected `}` or hexadecimal character.
  --> [14:2]
   |
14 | "\u{00B0";
   |  ^^^^^^^ Invalid escape sequence
'''

*/
"\u{00B0";
