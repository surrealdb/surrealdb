
/**

[env.capabilities]
allow-experimental = ["files"]

[test]

[test.results]
parsing-error = '''
Unexpected end of file string, missing bucket seperator `:/`
  --> [20:5]
   |
20 | f"fo";
   |     ^
'''


*/
f"fo";
