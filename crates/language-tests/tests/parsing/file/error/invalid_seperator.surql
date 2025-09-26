/**

[env.capabilities]
allow-experimental = ["files"]

[test]

[test.results]
parsing-error = '''
Unexpected character `b`, expected `/`
  --> [18:7]
   |
18 | f"foo:bar";
   |       ^
'''

*/
f"foo:bar";
