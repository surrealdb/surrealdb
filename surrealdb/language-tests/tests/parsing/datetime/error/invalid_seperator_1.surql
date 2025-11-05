/**
[test]

[test.results]
parsing-error = '''
Unexpected character `_`, expected datetime seperator characters `-`
  --> [14:10]
   |
14 | d"0000-01_01T00:00:00Z";
   |          ^
'''

*/
d"0000-01_01T00:00:00Z";
