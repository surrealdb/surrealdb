/**
[test]

[test.results]
parsing-error = '''
Unexpected character `b`, expected time seperator `T`
  --> [14:13]
   |
14 | d"0000-01-01b00:00:00Z";
   |             ^
'''

*/
d"0000-01-01b00:00:00Z";
