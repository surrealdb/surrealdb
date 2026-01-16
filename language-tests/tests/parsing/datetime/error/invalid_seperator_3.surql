/**
[test]

[test.results]
parsing-error = '''
Unexpected character `.`, expected datetime seperator characters `:`
  --> [14:16]
   |
14 | d"0000-01-01T00.00:00Z";
   |                ^
'''

*/
d"0000-01-01T00.00:00Z";
