/**
[test]

[test.results]
parsing-error = '''
Missing end brace `}` of unicode escape sequence
  --> [14:1]
   |
14 | "\u{00B0";
   | ^^^^^^^^^ 
'''

*/
"\u{00B0";
