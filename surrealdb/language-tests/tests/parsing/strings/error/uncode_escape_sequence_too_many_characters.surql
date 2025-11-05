/**
[test]

[test.results]
parsing-error = '''
Invalid escape sequence, expected `}` character.
  --> [14:11]
   |
14 | "\u{00B0000}";
   |           ^ Too many hex-digits
'''

*/
"\u{00B0000}";
