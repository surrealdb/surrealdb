/**
[test]

[test.results]
parsing-error = '''
Invalid datetime nanoseconds, expected no more then 9 digits
  --> [14:26]
   |
14 | d"-000000-01-01t00:00:00.0000000000+00:00";
   |                          ^^^^^^^^^^
'''

*/
d"-000000-01-01t00:00:00.0000000000+00:00";
