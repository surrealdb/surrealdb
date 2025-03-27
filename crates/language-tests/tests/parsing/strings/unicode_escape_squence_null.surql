/**
[test]

[test.results]
parsing-error = '''
Null bytes are not allowed in strings
  --> [14:1]
   |
14 | "\u{0000}";
   | ^^^^^^^^^ 
'''

*/
"\u{0000}";
