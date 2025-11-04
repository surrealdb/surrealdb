/**
[test]

[test.results]
parsing-error = '''
Invalid datetime, expected digit character found `b`
  --> [14:10]
   |
14 | d"-\u{30}b0000-01-01t00:00:00.000000000+00:00";
   |          ^
'''

*/
d"-\u{30}b0000-01-01t00:00:00.000000000+00:00";
