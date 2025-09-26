/**
[test]

[test.results]
parsing-error = '''
String contains invalid escape sequence, expected a hexadecimal character.
  --> [14:7]
   |
14 | "\u000G";
   |       ^ Unexpected character
'''

*/
"\u000G";
