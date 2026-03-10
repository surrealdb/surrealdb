/**
[test]

[test.results]
parsing-error = '''
Unexpected character `_`, expected datetime seperator characters `-`
  --> [14:7]
   |
14 | d"0000_01-01T00:00:00Z";
   |       ^
'''

*/
d"0000_01-01T00:00:00Z";
