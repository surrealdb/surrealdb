/**
[test]

[test.results]
parsing-error = '''
Invalid escape sequence
  --> [14:14]
   |
14 | "some string \j"
   |              ^^ not a valid escape character
'''

*/
"some string \j"
