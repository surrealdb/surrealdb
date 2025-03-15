/**
[test]

[test.results]
parsing-error = '''
Invalid character `"` in unicode escape sequence, must be a hex digit.
  --> [14:1]
   |
14 | "\u00B";
   | ^^^^^^^ 
'''

*/
"\u00B";
