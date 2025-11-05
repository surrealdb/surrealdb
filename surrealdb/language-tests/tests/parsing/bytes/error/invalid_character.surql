/**
[test]

[test.results]
parsing-error = '''
Unexpected character `h`, expected a hexidecimal digit
  --> [15:11]
   |
15 | b"03940302\u{68}0"
   |           ^^^^^^
'''

*/

b"03940302\u{68}0"
