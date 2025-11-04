/**
[test]

[test.results]
parsing-error = '''
Unexpected character `<`, expected datetime seperator characters `-`
  --> [15:15]
   |
15 | d"-\u{30}00000\u{3c}01-01t00:00:00.000000000+00:00";
   |               ^^^^^^
'''

*/

d"-\u{30}00000\u{3c}01-01t00:00:00.000000000+00:00";
