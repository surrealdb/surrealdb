/**
[env.capabilities]
allow-experimental = []

[test]
reason = "REMOVE MODULE is rejected at parse time when Surrealism modules are not enabled, with the caret on the MODULE keyword. Its parser arm is not compiled conditionally, so the diagnostic depends on the `surrealism` experimental capability alone and not on the `surrealism` cargo feature."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [18:8]
   |
18 | REMOVE MODULE mod::test;
   |        ^^^^^^ Use of `REMOVE MODULE` requires Surrealism modules
"""
*/

REMOVE MODULE mod::test;
