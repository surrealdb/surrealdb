/**
[env.capabilities]
allow-experimental = []

[test]
reason = "The `surrealism` capability gate on REMOVE MODULE fires before IF EXISTS is consumed, so IF EXISTS does not soften the rejection and the caret stays on the MODULE keyword rather than moving to IF."

[test.results]
parsing-error = """
Surrealism modules are not enabled
  --> [18:8]
   |
18 | REMOVE MODULE IF EXISTS mod::test;
   |        ^^^^^^ Use of `REMOVE MODULE` requires Surrealism modules
"""
*/

REMOVE MODULE IF EXISTS mod::test;
