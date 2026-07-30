/**
[env.capabilities]
allow-experimental = []

[test]
reason = "ALTER MODULE is rejected at parse time when Surrealism modules are not enabled, with the caret on the MODULE keyword. Its parser arm is not compiled conditionally, so the diagnostic depends on the `surrealism` experimental capability alone and not on the `surrealism` cargo feature."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [18:7]
   |
18 | ALTER MODULE mod::test COMMENT 'updated';
   |       ^^^^^^ Use of `ALTER MODULE` requires Surrealism modules
"""
*/

ALTER MODULE mod::test COMMENT 'updated';
