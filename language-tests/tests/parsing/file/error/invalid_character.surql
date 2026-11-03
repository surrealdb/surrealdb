/**

[test]

[test.results]
parsing-error = '''
Unexpected character `$`, file strings buckets only allow alpha numeric characters and `_`, `-`, and `.`
  --> [15:4]
   |
15 | f"f$o:/bar";
   |    ^
'''

*/
f"f$o:/bar";
