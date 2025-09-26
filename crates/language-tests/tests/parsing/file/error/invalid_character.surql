/**

[env.capabilities]
allow-experimental = ["files"]

[test]

[test.results]
parsing-error = '''
Unexpected character `$`, file strings buckets only allow alpha numeric characters and `_`, `-`, and `.`
  --> [18:4]
   |
18 | f"f$o:/bar";
   |    ^
'''

*/
f"f$o:/bar";
