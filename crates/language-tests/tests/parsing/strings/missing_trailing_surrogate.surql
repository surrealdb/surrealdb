/**
[test]

[test.results]
parsing-error = '''
Unicode escape sequence encoding a leading surrogate needs to be followed by a trailing surrogate
  --> [14:1]
   |
14 | "\uD83D";
   | ^^^^^^^^ 
'''

*/
"\uD83D";
